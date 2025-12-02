<?php

use App\Http\Controllers\UpdateController;
use Carbon\CarbonImmutable;

Route::get('exports/google-ads/{sourceKey}/conversions.csv', function (string $sourceKey) {
    $headers = [
        'Content-Type'        => 'text/csv; charset=UTF-8',
        'Content-Disposition' => 'inline; filename="conversions.csv"',
        'Cache-Control'       => 'no-store, no-cache, must-revalidate, max-age=0',
        'Pragma'              => 'no-cache',
    ];

    $columns = [
        'Conversion action',
        'GCLID',
        'Email',
        'Phone',
        'Conversion date and time',
        'Conversion value',
        'Currency',
        'Order ID',
        'WBRAID',
        'GBRAID',
        'User IP address',
        'User agent',
    ];

    $utc = CarbonImmutable::now('UTC');
    $rows = [
        // dummy rows
        [
            'Lead_Submit',
            'CjwK-EXAMPLE-GCLID-123',
            'alice@example.com',
            '+201234567890',
            $utc->toIso8601String(),
            '0',
            'USD',
            'LEAD-20001',
            '',
            '',
            '2a02:1234::abcd',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)',
        ],
        [
            'Purchase',
            '',
            'bob@example.com',
            '+971501234567',
            $utc->subDay()->toIso8601String(),
            '349.00',
            'USD',
            'ORD-90077',
            'wb_example',
            '',
            '',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
        ],
        [
            'Purchase',
            'CjwK-ANOTHER-GLID-456',
            '',
            '',
            $utc->subDays(2)->toIso8601String(),
            '299.99',
            'USD',
            'ORD-90078',
            '',
            'gb_example',
            '',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15)',
        ],
    ];

    return response()->stream(function () use ($columns, $rows) {
        $out = fopen('php://output', 'w');
        // UTF-8 BOM to help Sheets/Excel
        fwrite($out, "\xEF\xBB\xBF");
        fputcsv($out, $columns);
        foreach ($rows as $row) {
            fputcsv($out, $row);
        }
        fclose($out);
    }, 200, $headers);
});

Route::get('/wordpress-plugin/metadata.json', [UpdateController::class, 'pluginMetadata']);

require __DIR__.'/edges.php';
