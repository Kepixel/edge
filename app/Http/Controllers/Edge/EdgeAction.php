<?php

namespace App\Http\Controllers\Edge;

use App\Http\Controllers\Controller;
use Illuminate\Http\Request;

class EdgeAction extends Controller
{
    private static ?array $eventSchemaCache = null;

    public function __invoke(Request $request, $path = '')
    {
        return redirect('https://kepixel.com');
    }
}
