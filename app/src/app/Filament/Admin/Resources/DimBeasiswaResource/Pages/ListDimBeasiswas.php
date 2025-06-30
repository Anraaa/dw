<?php

namespace App\Filament\Admin\Resources\DimBeasiswaResource\Pages;

use App\Filament\Admin\Resources\DimBeasiswaResource;
use Filament\Actions;
use Filament\Resources\Pages\ListRecords;

class ListDimBeasiswas extends ListRecords
{
    protected static string $resource = DimBeasiswaResource::class;

    protected function getHeaderActions(): array
    {
        return [
            Actions\CreateAction::make(),
        ];
    }
}
