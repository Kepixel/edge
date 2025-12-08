<?php

namespace App\Console\Commands;

use App\Jobs\BuildUserJourneysJob;
use App\Jobs\CalculateAttributionJob;
use App\Jobs\UpdateJourneySummariesJob;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\Log;

class ProcessAttributionCommand extends Command
{
    protected $signature = 'attribution:process
                            {--team= : Process specific team only}
                            {--since= : Process events since this datetime (Y-m-d H:i:s)}
                            {--sync : Run synchronously instead of dispatching to queue}
                            {--step= : Run only a specific step (journeys|attribution|summaries)}';

    protected $description = 'Run the complete attribution pipeline (journeys -> attribution -> summaries)';

    public function handle(): int
    {
        $teamId = $this->option('team');
        $since = $this->option('since');
        $sync = $this->option('sync');
        $step = $this->option('step');

        $this->info('Starting Attribution Pipeline...');
        $this->newLine();

        if ($teamId) {
            $this->info("Team filter: {$teamId}");
        }

        if ($since) {
            $this->info("Processing since: {$since}");
        }

        $this->info('Mode: ' . ($sync ? 'Synchronous' : 'Queue'));
        $this->newLine();

        $steps = $step ? [$step] : ['journeys', 'attribution', 'summaries'];

        try {
            foreach ($steps as $currentStep) {
                $this->processStep($currentStep, $teamId, $since, $sync);
            }

            $this->newLine();
            $this->info('Attribution Pipeline completed successfully!');

            return self::SUCCESS;

        } catch (\Throwable $e) {
            $this->error('Attribution Pipeline failed: ' . $e->getMessage());
            Log::error('ProcessAttributionCommand: Pipeline failed', [
                'team_id' => $teamId,
                'step' => $step,
                'exception' => $e,
            ]);

            return self::FAILURE;
        }
    }

    private function processStep(string $step, ?string $teamId, ?string $since, bool $sync): void
    {
        switch ($step) {
            case 'journeys':
                $this->runJourneysStep($teamId, $since, $sync);
                break;

            case 'attribution':
                $this->runAttributionStep($teamId, $since, $sync);
                break;

            case 'summaries':
                $this->runSummariesStep($teamId, $since, $sync);
                break;

            default:
                $this->warn("Unknown step: {$step}. Valid steps: journeys, attribution, summaries");
        }
    }

    private function runJourneysStep(?string $teamId, ?string $since, bool $sync): void
    {
        $this->info('Step 1/3: Building User Journeys...');

        if ($sync) {
            BuildUserJourneysJob::dispatchSync($teamId, $since);
            $this->info('  -> Completed (sync)');
        } else {
            BuildUserJourneysJob::dispatch($teamId, $since);
            $this->info('  -> Dispatched to queue');
        }
    }

    private function runAttributionStep(?string $teamId, ?string $since, bool $sync): void
    {
        $this->info('Step 2/3: Calculating Attribution...');

        if ($sync) {
            CalculateAttributionJob::dispatchSync($teamId, $since);
            $this->info('  -> Completed (sync)');
        } else {
            CalculateAttributionJob::dispatch($teamId, $since);
            $this->info('  -> Dispatched to queue');
        }
    }

    private function runSummariesStep(?string $teamId, ?string $since, bool $sync): void
    {
        $this->info('Step 3/3: Updating Journey Summaries...');

        if ($sync) {
            UpdateJourneySummariesJob::dispatchSync($teamId, $since);
            $this->info('  -> Completed (sync)');
        } else {
            UpdateJourneySummariesJob::dispatch($teamId, $since);
            $this->info('  -> Dispatched to queue');
        }
    }
}
