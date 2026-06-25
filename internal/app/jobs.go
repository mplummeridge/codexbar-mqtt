package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/mplummeridge/codexbar-mqtt/internal/envelope"
)

type job struct {
	name         string
	interval     time.Duration
	initialDelay time.Duration
	run          func(context.Context) error
}

func (a *App) buildJobs() []job {
	var jobs []job
	add := func(name string, seconds int, delay time.Duration, fn func(context.Context) error) {
		if seconds <= 0 {
			return
		}
		jobs = append(jobs, job{name: name, interval: time.Duration(seconds) * time.Second, initialDelay: delay, run: fn})
	}

	add("serve-health", a.cfg.Poll.HealthSeconds, 0, func(ctx context.Context) error {
		return a.collectHTTP(ctx, "serve/health", "default", "machine_runtime_health", "/health", nil)
	})

	usageQuery := map[string]string{}
	usageScope := "enabled"
	if a.cfg.CodexBar.UsageProvider != "" {
		usageQuery["provider"] = a.cfg.CodexBar.UsageProvider
		usageScope = a.cfg.CodexBar.UsageProvider
	}
	add("serve-usage", a.cfg.Poll.UsageSeconds, time.Second, func(ctx context.Context) error {
		return a.collectHTTP(ctx, "serve/usage", usageScope, "serve_usage_snapshot", "/usage", usageQuery)
	})

	costQuery := map[string]string{}
	costScope := "enabled"
	if a.cfg.CodexBar.CostProvider != "" {
		costQuery["provider"] = a.cfg.CodexBar.CostProvider
		costScope = a.cfg.CodexBar.CostProvider
	}
	add("cost-attribution-cycle", a.cfg.Poll.CostSeconds, 3*time.Second, func(ctx context.Context) error {
		return a.runCostAttributionCycle(ctx, costScope, costQuery)
	})

	add("serve-all-providers", a.cfg.Poll.AllProvidersSeconds, 10*time.Second, func(ctx context.Context) error {
		return a.collectHTTP(ctx, "serve/usage", "all", "all_registered_provider_snapshot", "/usage", map[string]string{"provider": "all"})
	})

	statusProvider := a.cfg.Poll.StatusProvider
	if statusProvider == "" {
		statusProvider = "all"
	}
	add("cli-status", a.cfg.Poll.StatusSeconds, 15*time.Second, func(ctx context.Context) error {
		args := []string{"--provider", statusProvider, "--format", "json", "--json-only", "--status"}
		return a.collectCLI(ctx, "cli/usage-status", statusProvider, "provider_status_enriched_usage", args)
	})

	for i, provider := range a.cfg.Poll.ActiveAccountProbeProviders {
		provider := strings.TrimSpace(provider)
		if provider == "" {
			continue
		}
		name := "cli-active-account-" + provider
		delay := 5*time.Second + time.Duration(i)*2*time.Second
		add(name, a.cfg.Poll.ActiveAccountProbeSeconds, delay, func(ctx context.Context) error {
			args := []string{"--provider", provider, "--format", "json", "--json-only"}
			return a.collectCLI(ctx, "cli/active-account-probe", provider, "current_default_account_probe", args)
		})
	}

	for i, provider := range a.cfg.Poll.AccountCatalogueProviders {
		provider := strings.TrimSpace(provider)
		if provider == "" {
			continue
		}
		name := "cli-account-catalogue-" + provider
		delay := 20*time.Second + time.Duration(i)*3*time.Second
		add(name, a.cfg.Poll.AccountCatalogueSeconds, delay, func(ctx context.Context) error {
			args := []string{"--provider", provider, "--all-accounts", "--format", "json", "--json-only"}
			return a.collectCLI(ctx, "cli/account-catalogue", provider, "all_visible_or_configured_accounts", args)
		})
	}

	for i, days := range a.cfg.Poll.CostHorizonsDays {
		days := days
		name := fmt.Sprintf("cli-cost-%dd", days)
		scope := fmt.Sprintf("%s-%dd", costScope, days)
		delay := 30*time.Second + time.Duration(i)*3*time.Second
		add(name, a.cfg.Poll.CostHorizonSeconds, delay, func(ctx context.Context) error {
			args := []string{"cost"}
			if a.cfg.CodexBar.CostProvider != "" {
				args = append(args, "--provider", a.cfg.CodexBar.CostProvider)
			}
			args = append(args, "--days", strconv.Itoa(days), "--format", "json", "--json-only")
			return a.collectCLI(ctx, "cli/cost-horizon", scope, "machine_local_cost_history", args)
		})
	}

	add("cli-config-validate", a.cfg.Poll.ConfigValidateSeconds, 45*time.Second, func(ctx context.Context) error {
		return a.collectCLI(ctx, "cli/config-validation", "default", "local_codexbar_config_validation", []string{"config", "validate", "--format", "json", "--json-only"})
	})
	return jobs
}

func (a *App) scheduleJob(ctx context.Context, job job) {
	if job.initialDelay > 0 {
		timer := time.NewTimer(job.initialDelay)
		select {
		case <-timer.C:
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return
		}
	}
	_ = a.executeJob(ctx, job)
	ticker := time.NewTicker(job.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			_ = a.executeJob(ctx, job)
		case <-ctx.Done():
			return
		}
	}
}

func (a *App) executeJob(ctx context.Context, job job) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("job panic: %v", recovered)
		}
		a.updateJob(job.name, err)
		if err != nil {
			a.logger.Warn("collection job failed", "job", job.name, "error", err)
		}
	}()
	return job.run(ctx)
}

func (a *App) runCostAttributionCycle(ctx context.Context, costScope string, costQuery map[string]string) error {
	cycleID := fmt.Sprintf("cost-%s-%d", a.cfg.Machine.ID, time.Now().UTC().UnixNano())
	var errs []error
	providers := make([]string, 0, len(a.cfg.Poll.ActiveAccountProbeProviders))
	seen := map[string]bool{}
	for _, provider := range a.cfg.Poll.ActiveAccountProbeProviders {
		provider = strings.TrimSpace(provider)
		if provider == "" || seen[provider] {
			continue
		}
		seen[provider] = true
		providers = append(providers, provider)
	}
	for _, provider := range providers {
		args := []string{"--provider", provider, "--format", "json", "--json-only"}
		if err := a.collectCLIMeta(ctx, "cli/active-account-probe", provider, "cost_attribution_account_bracket", args, collectionMeta{CorrelationID: cycleID, Phase: "before-cost"}); err != nil {
			errs = append(errs, err)
		}
	}
	if err := a.collectHTTPMeta(ctx, "serve/cost", costScope, "machine_local_cost_snapshot", "/cost", costQuery, collectionMeta{CorrelationID: cycleID, Phase: "cost"}); err != nil {
		errs = append(errs, err)
	}
	for _, provider := range providers {
		args := []string{"--provider", provider, "--format", "json", "--json-only"}
		if err := a.collectCLIMeta(ctx, "cli/active-account-probe", provider, "cost_attribution_account_bracket", args, collectionMeta{CorrelationID: cycleID, Phase: "after-cost"}); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

type collectionMeta struct {
	CorrelationID string
	Phase         string
}

func (a *App) collectHTTP(ctx context.Context, kind, scope, semanticScope, path string, query map[string]string) error {
	return a.collectHTTPMeta(ctx, kind, scope, semanticScope, path, query, collectionMeta{})
}

func (a *App) collectHTTPMeta(ctx context.Context, kind, scope, semanticScope, path string, query map[string]string, meta collectionMeta) error {
	response, fetchErr := a.http.Fetch(ctx, path, query)
	collection := envelope.Collection{
		Transport: "http", Operation: strings.TrimPrefix(path, "/"), SemanticScope: semanticScope,
		CorrelationID: meta.CorrelationID, Phase: meta.Phase,
		StartedAt: response.StartedAt, FinishedAt: response.FinishedAt,
		Endpoint: path, Query: cloneMap(query), DurationMS: response.Duration.Milliseconds(),
		ContentType: response.ContentType, Success: fetchErr == nil,
	}
	if fetchErr != nil {
		collection.Error = fetchErr.Error()
	}
	if len(response.Payload) > 0 && json.Valid(response.Payload) {
		obs := envelope.New(kind, scope, a.machine, a.agent, collection, response.Payload)
		if err := a.enqueueObservation(obs); err != nil {
			return errors.Join(fetchErr, err)
		}
		return fetchErr
	}
	obs := envelope.NewError("agent/error", kind+"-"+scope, a.machine, a.agent, collection, kind, fetchErr, true)
	if err := a.enqueueObservation(obs); err != nil {
		return errors.Join(fetchErr, err)
	}
	return fetchErr
}

func (a *App) collectCLI(ctx context.Context, kind, scope, semanticScope string, args []string) error {
	return a.collectCLIMeta(ctx, kind, scope, semanticScope, args, collectionMeta{})
}

func (a *App) collectCLIMeta(ctx context.Context, kind, scope, semanticScope string, args []string, meta collectionMeta) error {
	a.cliMu.Lock()
	response, runErr := a.runner.RunJSON(ctx, args...)
	a.cliMu.Unlock()
	exitCode := response.ExitCode
	collection := envelope.Collection{
		Transport: "cli", Operation: cliOperation(args), SemanticScope: semanticScope,
		CorrelationID: meta.CorrelationID, Phase: meta.Phase,
		StartedAt: response.StartedAt, FinishedAt: response.FinishedAt,
		Command: response.Command, ExitCode: &exitCode, DurationMS: response.Duration.Milliseconds(),
		ContentType: "application/json", Success: runErr == nil,
	}
	if runErr != nil {
		collection.Error = runErr.Error()
	}
	if a.cfg.Publish.IncludeStderr {
		collection.Stderr = response.Stderr
	}
	if len(response.Payload) > 0 && json.Valid(response.Payload) {
		obs := envelope.New(kind, scope, a.machine, a.agent, collection, response.Payload)
		if err := a.enqueueObservation(obs); err != nil {
			return errors.Join(runErr, err)
		}
		return runErr
	}
	obs := envelope.NewError("agent/error", kind+"-"+scope, a.machine, a.agent, collection, kind, runErr, true)
	if err := a.enqueueObservation(obs); err != nil {
		return errors.Join(runErr, err)
	}
	return runErr
}

func cloneMap(input map[string]string) map[string]string {
	if len(input) == 0 {
		return nil
	}
	output := make(map[string]string, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}

func cliOperation(args []string) string {
	if len(args) == 0 || strings.HasPrefix(args[0], "-") {
		return "usage"
	}
	return args[0]
}

func encodedQuery(query map[string]string) string {
	values := url.Values{}
	for key, value := range query {
		values.Set(key, value)
	}
	return values.Encode()
}
