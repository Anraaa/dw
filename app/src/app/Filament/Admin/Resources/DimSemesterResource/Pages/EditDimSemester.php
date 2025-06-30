<?php

namespace App\Filament\Admin\Resources\DimSemesterResource\Pages;

use App\Filament\Admin\Resources\DimSemesterResource;
use Filament\Actions;
use Filament\Resources\Pages\EditRecord;

class EditDimSemester extends EditRecord
{
    protected static string $resource = DimSemesterResource::class;

    protected function getHeaderActions(): array
    {
        return [
            Actions\DeleteAction::make(),
        ];
    }
}
