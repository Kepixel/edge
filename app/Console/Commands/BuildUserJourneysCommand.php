<?php

namespace App\Console\Commands;

use App\Jobs\BuildUserJourneysJob;
use Illuminate\Console\Command;

class BuildUserJourneysCommand extends Command
{
    protected $signature = 'attribution:build-journeys
                            {--team= : Process specific team only}
                            {--since= : Process events since this datetime (Y-m-d H:i:s)}
                            {--sync : Run synchronously instead of dispatching to queue}';

    protected $description = 'Build user journeys from event_enriched data';

    public function handle(): int
    {
        $teamId = $this->option('team');
        $since = $this->option('since');
        $sync = $this->option('sync');

        $this->info('Building user journeys...');

        if ($teamId) {
            $this->info("Filtering by team: {$teamId}");
        }

        if ($since) {
            $this->info("Processing events since: {$since}");
        }

        $job = new BuildUserJourneysJob($teamId, $since);

        if ($sync) {
            $this->info('Running synchronously...');
            BuildUserJourneysJob::dispatchSync($teamId, $since);
        } else {
            $this->info('Dispatching to queue...');
            BuildUserJourneysJob::dispatch($teamId, $since);
        }

        $this->info('Done!');

        return self::SUCCESS;
    }
}
