<?php

namespace App\Http\Controllers;

class UpdateController extends Controller
{
    public function pluginMetadata()
    {
        return response()->json([
            'name' => 'Anubis',
        ]);
    }
}
