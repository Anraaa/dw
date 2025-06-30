<?php

namespace App\Filament\Admin\Resources\DimDosenResource\Pages;

use App\Filament\Admin\Resources\DimDosenResource;
use Filament\Actions;
use Filament\Resources\Pages\ListRecords;

class ListDimDosens extends ListRecords
{
    protected static string $resource = DimDosenResource::class;

    protected function getHeaderActions(): array
    {
        return [
            Actions\CreateAction::make(),
        ];
    }
}
