<?php

namespace App\Filament\Admin\Resources\FaktaIpkResource\Pages;

use App\Filament\Admin\Resources\FaktaIpkResource;
use Filament\Actions;
use Filament\Resources\Pages\ListRecords;

class ListFaktaIpks extends ListRecords
{
    protected static string $resource = FaktaIpkResource::class;

    protected function getHeaderActions(): array
    {
        return [
            Actions\CreateAction::make(),
        ];
    }
}
