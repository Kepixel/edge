<?php

namespace App\Console\Commands;

use App\Jobs\UpdateJourneySummariesJob;
use Illuminate\Console\Command;

class UpdateJourneySummariesCommand extends Command
{
    protected $signature = 'attribution:update-summaries
                            {--team= : Process specific team only}
                            {--since= : Process journeys updated since this datetime (Y-m-d H:i:s)}
                            {--sync : Run synchronously instead of dispatching to queue}';

    protected $description = 'Update user journey summaries from touchpoint data';

    public function handle(): int
    {
        $teamId = $this->option('team');
        $since = $this->option('since');
        $sync = $this->option('sync');

        $this->info('Updating journey summaries...');

        if ($teamId) {
            $this->info("Filtering by team: {$teamId}");
        }

        if ($since) {
            $this->info("Processing journeys since: {$since}");
        }

        if ($sync) {
            $this->info('Running synchronously...');
            UpdateJourneySummariesJob::dispatchSync($teamId, $since);
        } else {
            $this->info('Dispatching to queue...');
            UpdateJourneySummariesJob::dispatch($teamId, $since);
        }

        $this->info('Done!');

        return self::SUCCESS;
    }
}
