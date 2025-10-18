<?php

namespace App\Http\Controllers\Edge;

use App\Http\Controllers\Controller;
use App\Jobs\ProcessMatomoRequest;
use App\Jobs\ProcessRudderRequest;
use Illuminate\Http\Request;

class EdgeAction extends Controller
{
    private static ?array $eventSchemaCache = null;

    public function __invoke(Request $request, $path = '')
    {
        if ($request->has('properties')) {
            $properties = $request->get('properties');
            if (isset($properties['client_dedup_id'])) {
                return response()->json([
                    'ok' => true,
                ], 202);
            }
        }

        if ($path) {
            $authHeader = $request->header('Authorization');

            if ($authHeader && str_starts_with($authHeader, 'Basic ')) {
                $base64Credentials = substr($authHeader, 6);
                $decoded = base64_decode($base64Credentials);

                $ip = explode(',', $request->header('X-Forwarded-For'))[0] ?? $request->ip();

                $headers = [
                    'Accept' => $request->header('Accept'),
                    'Accept-Encoding' => $request->header('Accept-Encoding'),
                    'Accept-Language' => $request->header('Accept-Language'),
                    'Referer' => $request->header('Referer'),
                    'Origin' => $request->header('Origin'),
                    'Content-Type' => $request->header('Content-Type'),
                    'Content-Length' => $request->header('Content-Length'),
                    'Connection' => $request->header('Connection'),
                    'Cookie' => $request->header('Cookie'),
                    'x-client-ip' => $ip,
                    'X-Forwarded-For' => $ip,
                    'anonymousid' => $request->header('anonymousid'),
                    'User-Agent' => $request->header('User-Agent'),
                ];

                $paths = [
                    'v1/i' => 'v1/identify',
                    'v1/t' => 'v1/track',
                    'v1/p' => 'v1/page',
                    'v1/s' => 'v1/screen',
                    'v1/g' => 'v1/group',
                    'v1/a' => 'v1/alias',
                    'v1/b' => 'v1/batch',
                ];

                $path = $paths[$path] ?? $path;

                if ($this->isTrackEvent($path)) {
                    $event = $request->get('event');

                    if (! is_string($event) || trim($event) === '') {
                        return response()->json([
                            'ok' => false,
                            'errors' => ['event is required for track requests'],
                        ], 422);
                    }

                    $properties = $request->get('properties', []);

                    if (! is_array($properties)) {
                        return response()->json([
                            'ok' => false,
                            'errors' => ['properties must be an object'],
                        ], 422);
                    }

                    $validationErrors = $this->validateEventProperties($event, $properties);

                    if (! empty($validationErrors)) {
                        return response()->json([
                            'ok' => false,
                            'errors' => $validationErrors,
                        ], 422);
                    }
                }

                ProcessRudderRequest::dispatch(\Str::beforeLast($decoded, ':'), $request->all(), $headers, $path);
            }

            return response()->json([
                'ok' => true,
            ], 202);
        }
        $code = 204;
        if ($request->has('appid')) {
            //            $ip = explode(',', $request->header('X-Forwarded-For'))[0] ?? $request->ip();
            //
            //            $data = $request->all();
            //            $data['cip'] = $ip;
            //            $data['ua'] = $request->header('User-Agent');
            //
            //            $headers = [
            //                'Accept' => $request->header('Accept'),
            //                'Accept-Encoding' => $request->header('Accept-Encoding'),
            //                'Accept-Language' => $request->header('Accept-Language'),
            //                'Referer' => $request->header('Referer'),
            //                'Origin' => $request->header('Origin'),
            //                'Content-Type' => $request->header('Content-Type'),
            //                'Content-Length' => $request->header('Content-Length'),
            //                'Connection' => $request->header('Connection'),
            //                'Cookie' => $request->header('Cookie'),
            //                'x-client-ip' => $ip,
            //                'X-Forwarded-For' => $ip,
            //            ];

            // Dispatch the job to process the request in the background
            //            ProcessMatomoRequest::dispatch(
            //                request('appid'),
            //                $data,
            //                $headers,
            //                $request->header('User-Agent')
            //            );

            // Return 202 Accepted status code to indicate the request has been accepted for processing
            $code = 202;
        }

        return response(null, $code);
    }

    /**
     * Check if the current request is a track event.
     */
    private function isTrackEvent($path): bool
    {
        return $path === 'v1/t' || $path === 'v1/track';
    }

    private function validateEventProperties(string $eventName, array $properties): array
    {
        $schemas = $this->loadEventSchemas();

        $eventSchema = $schemas['events'][$eventName] ?? null;

        if (! $eventSchema) {
            return [sprintf('Unknown event "%s"', $eventName)];
        }

        $schemaForProperties = $eventSchema;

        if (isset($schemaForProperties['properties']['event']['const']) &&
            $schemaForProperties['properties']['event']['const'] !== $eventName) {
            return [sprintf('Event payload must match "%s"', $schemaForProperties['properties']['event']['const'])];
        }

        if (isset($schemaForProperties['properties']['event'])) {
            unset($schemaForProperties['properties']['event']);
        }

        if (isset($schemaForProperties['required'])) {
            $schemaForProperties['required'] = array_values(array_filter(
                $schemaForProperties['required'],
                static fn ($field) => $field !== 'event'
            ));
        }

        return $this->validateObjectAgainstSchema(
            $properties,
            $schemaForProperties,
            $schemas['definitions'],
            'properties'
        );
    }

    private function validateObjectAgainstSchema(array $data, array $schema, array $definitions, string $context): array
    {
        $errors = [];

        if (($schema['type'] ?? null) !== 'object') {
            return $errors;
        }

        foreach ($schema['required'] ?? [] as $field) {
            if (! array_key_exists($field, $data) || $data[$field] === null) {
                $errors[] = sprintf('%s is required', $this->contextPath($context, $field));
            }
        }

        $propertiesSchema = $schema['properties'] ?? [];

        foreach ($data as $key => $value) {
            if (! array_key_exists($key, $propertiesSchema)) {
                if (($schema['additionalProperties'] ?? true) === false) {
                    $errors[] = sprintf('%s is not allowed', $this->contextPath($context, $key));
                }

                continue;
            }

            $propSchema = $propertiesSchema[$key];
            $propContext = $this->contextPath($context, $key);
            $errors = array_merge(
                $errors,
                $this->validateValueAgainstSchema($value, $propSchema, $definitions, $propContext)
            );
        }

        return $errors;
    }

    private function validateValueAgainstSchema($value, array $schema, array $definitions, string $context): array
    {
        $errors = [];

        if (isset($schema['const']) && $value !== $schema['const']) {
            $errors[] = sprintf('%s must be %s', $context, json_encode($schema['const']));

            return $errors;
        }

        $type = $schema['type'] ?? null;

        if ($type === 'string') {
            if (! is_string($value)) {
                $errors[] = sprintf('%s must be a string', $context);
            } elseif (isset($schema['format'])) {
                $errors = array_merge($errors, $this->validateStringFormat($value, $schema['format'], $context));
            }

            return $errors;
        }

        if ($type === 'number') {
            if (! is_numeric($value)) {
                $errors[] = sprintf('%s must be a number', $context);
            }

            return $errors;
        }

        if ($type === 'integer') {
            if (! $this->isInteger($value)) {
                $errors[] = sprintf('%s must be an integer', $context);
            }

            return $errors;
        }

        if ($type === 'boolean') {
            if (! is_bool($value)) {
                $errors[] = sprintf('%s must be a boolean', $context);
            }

            return $errors;
        }

        if ($type === 'array') {
            if (! is_array($value)) {
                $errors[] = sprintf('%s must be an array', $context);

                return $errors;
            }

            if (isset($schema['minItems']) && count($value) < $schema['minItems']) {
                $errors[] = sprintf('%s must contain at least %d item(s)', $context, $schema['minItems']);
            }

            $itemsSchema = $schema['items'] ?? null;

            if ($itemsSchema) {
                foreach (array_values($value) as $index => $item) {
                    $itemContext = $this->contextPath($context, $index);
                    $errors = array_merge(
                        $errors,
                        $this->validateArrayItem($item, $itemsSchema, $definitions, $itemContext)
                    );
                }
            }

            return $errors;
        }

        if ($type === 'object') {
            if (! is_array($value)) {
                $errors[] = sprintf('%s must be an object', $context);

                return $errors;
            }

            return array_merge(
                $errors,
                $this->validateObjectAgainstSchema($value, $schema, $definitions, $context)
            );
        }

        if (isset($schema['$ref'])) {
            $refSchema = $this->resolveDefinition($schema['$ref'], $definitions);

            if (! $refSchema) {
                return [sprintf('Unable to resolve schema reference %s', $schema['$ref'])];
            }

            if (! is_array($value)) {
                return [sprintf('%s must be an object', $context)];
            }

            return $this->validateObjectAgainstSchema($value, $refSchema, $definitions, $context);
        }

        return $errors;
    }

    private function validateArrayItem($value, array $schema, array $definitions, string $context): array
    {
        if (isset($schema['$ref'])) {
            $refSchema = $this->resolveDefinition($schema['$ref'], $definitions);

            if (! $refSchema) {
                return [sprintf('Unable to resolve schema reference %s', $schema['$ref'])];
            }

            if (! is_array($value)) {
                return [sprintf('%s must be an object', $context)];
            }

            return $this->validateObjectAgainstSchema($value, $refSchema, $definitions, $context);
        }

        return $this->validateValueAgainstSchema($value, $schema, $definitions, $context);
    }

    private function validateStringFormat(string $value, string $format, string $context): array
    {
        if ($format === 'email' && filter_var($value, FILTER_VALIDATE_EMAIL) === false) {
            return [sprintf('%s must be a valid email address', $context)];
        }

        if ($format === 'uri' && filter_var($value, FILTER_VALIDATE_URL) === false) {
            return [sprintf('%s must be a valid URL', $context)];
        }

        return [];
    }

    private function isInteger($value): bool
    {
        if (is_int($value)) {
            return true;
        }

        if (is_string($value) && preg_match('/^-?\\d+$/', $value)) {
            return true;
        }

        return is_numeric($value) && (int) $value == $value;
    }

    private function resolveDefinition(string $reference, array $definitions): ?array
    {
        if (! str_starts_with($reference, '#/$defs/')) {
            return null;
        }

        $key = substr($reference, strlen('#/$defs/'));

        return $definitions[$key] ?? null;
    }

    private function contextPath(string $context, $key): string
    {
        if ($key === 'event') {
            return 'event';
        }

        if ($context === '') {
            return (string) $key;
        }

        if (is_int($key)) {
            return sprintf('%s[%d]', $context, $key);
        }

        return sprintf('%s.%s', $context, $key);
    }

    private function loadEventSchemas(): array
    {
        if (self::$eventSchemaCache !== null) {
            return self::$eventSchemaCache;
        }

        $path = base_path('events.schema.optimized.json');

        if (! is_readable($path)) {
            throw new \RuntimeException('Unable to read events schema file.');
        }

        $schemaContent = file_get_contents($path);
        $decoded = json_decode($schemaContent, true);

        if (! is_array($decoded)) {
            throw new \RuntimeException('Invalid events schema file.');
        }

        $events = [];

        foreach ($decoded['oneOf'] ?? [] as $eventSchema) {
            if (isset($eventSchema['title'])) {
                $events[$eventSchema['title']] = $eventSchema;
            }
        }

        self::$eventSchemaCache = [
            'events' => $events,
            'definitions' => $decoded['$defs'] ?? [],
        ];

        return self::$eventSchemaCache;
    }
}
