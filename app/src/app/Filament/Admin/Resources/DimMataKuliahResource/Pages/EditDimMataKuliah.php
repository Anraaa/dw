<?php

namespace App\Filament\Admin\Resources\DimMataKuliahResource\Pages;

use App\Filament\Admin\Resources\DimMataKuliahResource;
use Filament\Actions;
use Filament\Resources\Pages\EditRecord;

class EditDimMataKuliah extends EditRecord
{
    protected static string $resource = DimMataKuliahResource::class;

    protected function getHeaderActions(): array
    {
        return [
            Actions\DeleteAction::make(),
        ];
    }
}
