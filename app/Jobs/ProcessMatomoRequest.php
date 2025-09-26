<?php

namespace App\Jobs;

use App\Models\Source;
use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\SerializesModels;
use Illuminate\Support\Facades\Http;
use Illuminate\Support\Facades\Log;

class ProcessMatomoRequest implements ShouldQueue
{
    use Dispatchable, InteractsWithQueue, Queueable, SerializesModels;

    protected $sourceKey;

    protected $data;

    protected $headers;

    protected $userAgent;

    /**
     * Create a new job instance.
     *
     * @return void
     */
    public function __construct($sourceKey, $data, $headers, $userAgent)
    {
        $this->sourceKey = $sourceKey;
        $this->data = $data;
        $this->headers = $headers;
        $this->userAgent = $userAgent;
    }

    /**
     * Execute the job.
     *
     * @return void
     */
    public function handle()
    {
        try {
            $source = Source::where('app_token', $this->sourceKey)->first();
            if (! $source) {
                Log::warning('Source not found for token: '.$this->sourceKey);

                return;
            }

            $url = config('services.terminal.base_url');
            $endpoint = "$url/matomo.php?rec=1&idsite=".$source->site_id.'&token_auth='.config('services.terminal.api_key');

            $response = Http::withUserAgent($this->userAgent)
                ->withHeaders($this->headers)
                ->asForm()
                ->post($endpoint, $this->data);

            if ($response->failed()) {
                Log::error('Failed to send data to Matomo: '.$response->status(), [
                    'source_key' => $this->sourceKey,
                    'status' => $response->status(),
                    'response' => $response->body(),
                ]);
            }
        } catch (\Exception $e) {
            Log::error('Error processing Matomo request: '.$e->getMessage(), [
                'source_key' => $this->sourceKey,
                'exception' => $e,
            ]);
        }
    }
}
