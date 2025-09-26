<?php

namespace App\Http\Controllers\Edge;

use App\Http\Controllers\Controller;
use Illuminate\Http\Response;

class EdgeOptionsController extends Controller
{
    public function __invoke(): Response
    {
        return response()->noContent();
    }
}
