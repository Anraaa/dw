<?php

namespace App\Filament\Admin\Resources\FaktaNilaiResource\Pages;

use App\Filament\Admin\Resources\FaktaNilaiResource;
use Filament\Actions;
use Filament\Resources\Pages\EditRecord;

class EditFaktaNilai extends EditRecord
{
    protected static string $resource = FaktaNilaiResource::class;

    protected function getHeaderActions(): array
    {
        return [
            Actions\DeleteAction::make(),
        ];
    }
}
