<?php

namespace App\Filament\Admin\Resources\DimDosenResource\Pages;

use App\Filament\Admin\Resources\DimDosenResource;
use Filament\Actions;
use Filament\Resources\Pages\EditRecord;

class EditDimDosen extends EditRecord
{
    protected static string $resource = DimDosenResource::class;

    protected function getHeaderActions(): array
    {
        return [
            Actions\DeleteAction::make(),
        ];
    }
}
