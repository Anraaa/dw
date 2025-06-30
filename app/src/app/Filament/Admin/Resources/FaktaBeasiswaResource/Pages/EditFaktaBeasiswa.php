<?php

namespace App\Filament\Admin\Resources\FaktaBeasiswaResource\Pages;

use App\Filament\Admin\Resources\FaktaBeasiswaResource;
use Filament\Actions;
use Filament\Resources\Pages\EditRecord;

class EditFaktaBeasiswa extends EditRecord
{
    protected static string $resource = FaktaBeasiswaResource::class;

    protected function getHeaderActions(): array
    {
        return [
            Actions\DeleteAction::make(),
        ];
    }
}
