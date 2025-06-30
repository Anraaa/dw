<?php

namespace App\Filament\Admin\Resources\FaktaIpkResource\Pages;

use App\Filament\Admin\Resources\FaktaIpkResource;
use Filament\Actions;
use Filament\Resources\Pages\EditRecord;

class EditFaktaIpk extends EditRecord
{
    protected static string $resource = FaktaIpkResource::class;

    protected function getHeaderActions(): array
    {
        return [
            Actions\DeleteAction::make(),
        ];
    }
}
