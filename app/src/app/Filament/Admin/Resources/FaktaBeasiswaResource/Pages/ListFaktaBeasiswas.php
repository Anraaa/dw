<?php

namespace App\Filament\Admin\Resources\FaktaBeasiswaResource\Pages;

use App\Filament\Admin\Resources\FaktaBeasiswaResource;
use Filament\Actions;
use Filament\Resources\Pages\ListRecords;

class ListFaktaBeasiswas extends ListRecords
{
    protected static string $resource = FaktaBeasiswaResource::class;

    protected function getHeaderActions(): array
    {
        return [
            Actions\CreateAction::make(),
        ];
    }
}
