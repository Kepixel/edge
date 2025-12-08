<?php

namespace App\Console\Commands;

use App\Jobs\CalculateAttributionJob;
use Illuminate\Console\Command;

class CalculateAttributionCommand extends Command
{
    protected $signature = 'attribution:calculate
                            {--team= : Process specific team only}
                            {--since= : Process conversions since this datetime (Y-m-d H:i:s)}
                            {--sync : Run synchronously instead of dispatching to queue}';

    protected $description = 'Calculate attribution for conversions';

    public function handle(): int
    {
        $teamId = $this->option('team');
        $since = $this->option('since');
        $sync = $this->option('sync');

        $this->info('Calculating attribution...');

        if ($teamId) {
            $this->info("Filtering by team: {$teamId}");
        }

        if ($since) {
            $this->info("Processing conversions since: {$since}");
        }

        if ($sync) {
            $this->info('Running synchronously...');
            CalculateAttributionJob::dispatchSync($teamId, $since);
        } else {
            $this->info('Dispatching to queue...');
            CalculateAttributionJob::dispatch($teamId, $since);
        }

        $this->info('Done!');

        return self::SUCCESS;
    }
}
