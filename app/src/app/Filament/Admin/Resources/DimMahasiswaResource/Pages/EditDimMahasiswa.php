<?php

namespace App\Filament\Admin\Resources\DimMahasiswaResource\Pages;

use App\Filament\Admin\Resources\DimMahasiswaResource;
use Filament\Actions;
use Filament\Resources\Pages\EditRecord;

class EditDimMahasiswa extends EditRecord
{
    protected static string $resource = DimMahasiswaResource::class;

    protected function getHeaderActions(): array
    {
        return [
            Actions\DeleteAction::make(),
        ];
    }
}
