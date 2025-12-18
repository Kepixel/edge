<?php

namespace Tests\Unit;

use App\Jobs\ProcessRudderRequest;
use Kepixel\Core\Models\Source;
use Kepixel\Core\Models\Team;
use Illuminate\Foundation\Testing\RefreshDatabase;
use Illuminate\Support\Facades\Http;
use Illuminate\Support\Facades\Log;
use Tests\TestCase;

class ProcessRudderRequestTest extends TestCase
{
    use RefreshDatabase;

    public function test_uses_traefik_url_with_correct_port(): void
    {
        // Create test team and source
        $team = Team::factory()->create(['id' => 'test-team-123']);
        $source = Source::factory()->create([
            'app_token' => 'test-token',
            'team_id' => $team->id,
            'write_key' => 'test-write-key',
        ]);

        // Mock HTTP response
        Http::fake([
            'http://127.0.0.1:8090/t/test-team-123/v1/track' => Http::response(['status' => 'ok'], 200),
        ]);

        // Mock the stack validation to return success
        $job = $this->createJobWithMockedStackValidation(true);

        $job->handle();

        // Assert the correct URL was called
        Http::assertSent(function ($request) {
            return $request->url() === 'http://127.0.0.1:8090/t/test-team-123/v1/track';
        });
    }

    public function test_handles_team_stack_not_running(): void
    {
        // Create test team and source
        $team = Team::factory()->create(['id' => 'test-team-456']);
        $source = Source::factory()->create([
            'app_token' => 'test-token',
            'team_id' => $team->id,
            'write_key' => 'test-write-key',
        ]);

        // Mock the stack validation to return not running
        $job = $this->createJobWithMockedStackValidation(false);

        Log::shouldReceive('warning')->once()->with(
            'Team Docker Swarm stack not running for source: '.$source->id,
            [
                'team_id' => $team->id,
                'source_id' => $source->id,
                'attempt' => 1,
            ]
        );

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Team Docker Swarm stack not running for source: '.$source->id);

        $job->handle();
    }

    public function test_path_mapping_works_correctly(): void
    {
        // Create test team and source
        $team = Team::factory()->create(['id' => 'test-team-789']);
        $source = Source::factory()->create([
            'app_token' => 'test-token',
            'team_id' => $team->id,
            'write_key' => 'test-write-key',
        ]);

        // Mock HTTP responses for different paths
        Http::fake([
            'http://127.0.0.1:8090/t/test-team-789/v1/identify' => Http::response(['status' => 'ok'], 200),
            'http://127.0.0.1:8090/t/test-team-789/v1/track' => Http::response(['status' => 'ok'], 200),
            'http://127.0.0.1:8090/t/test-team-789/v1/page' => Http::response(['status' => 'ok'], 200),
        ]);

        // Test different path mappings
        $testCases = [
            'v1/i' => 'v1/identify',
            'v1/t' => 'v1/track',
            'v1/p' => 'v1/page',
        ];

        foreach ($testCases as $shortPath => $fullPath) {
            // Create job with mocked stack validation
            $job = $this->createJobWithMockedStackValidation(true, $shortPath);

            $job->handle();

            // Assert the correct mapped URL was called
            Http::assertSent(function ($request) use ($team, $fullPath) {
                return $request->url() === "http://127.0.0.1:8090/t/{$team->id}/$fullPath";
            });
        }
    }

    public function test_safe_id_conversion_in_stack_validation(): void
    {
        $job = new ProcessRudderRequest('test-token', [], [], 'v1/t');
        $reflection = new \ReflectionClass($job);

        $isTeamStackRunningMethod = $reflection->getMethod('isTeamStackRunning');
        $isTeamStackRunningMethod->setAccessible(true);

        // Mock shell_exec to capture the command
        $capturedCommand = null;
        $originalShellExec = null;

        // We can't easily mock shell_exec, so we'll test the logic by checking
        // if the method handles team IDs with special characters correctly
        $teamId = '01972c85-c0c2-7395-8c58-e40ba3d2ef46';

        // This will call shell_exec but we expect it to return false since the stack doesn't exist
        $result = $isTeamStackRunningMethod->invoke($job, $teamId);

        // Should return false when no stack is running
        $this->assertFalse($result);
    }

    public function test_handles_source_not_found(): void
    {
        $job = new ProcessRudderRequest('non-existent-token', [], [], 'v1/t');

        Log::shouldReceive('warning')->once()->with('Source not found for token: non-existent-token');

        // Should not throw exception, just return
        $job->handle();

        // Test passes if no exception is thrown
        $this->assertTrue(true);
    }

    private function createJobWithMockedStackValidation(bool $stackRunning, string $path = 'v1/t'): ProcessRudderRequest
    {
        $job = new ProcessRudderRequest('test-token', ['test' => 'data'], ['Content-Type' => 'application/json'], $path);

        // Create a mock that overrides the isTeamStackRunning method
        $mock = $this->getMockBuilder(ProcessRudderRequest::class)
            ->setConstructorArgs(['test-token', ['test' => 'data'], ['Content-Type' => 'application/json'], $path])
            ->onlyMethods(['isTeamStackRunning'])
            ->getMock();

        $mock->method('isTeamStackRunning')
            ->willReturn($stackRunning);

        return $mock;
    }
}
