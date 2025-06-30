<?php

namespace App\Filament\Admin\Resources\DimMataKuliahResource\Pages;

use App\Filament\Admin\Resources\DimMataKuliahResource;
use Filament\Actions;
use Filament\Resources\Pages\ListRecords;

class ListDimMataKuliahs extends ListRecords
{
    protected static string $resource = DimMataKuliahResource::class;

    protected function getHeaderActions(): array
    {
        return [
            Actions\CreateAction::make(),
        ];
    }
}
