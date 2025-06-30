<?php

namespace App\Filament\Admin\Resources\FaktaNilaiResource\Pages;

use App\Filament\Admin\Resources\FaktaNilaiResource;
use Filament\Actions;
use Filament\Resources\Pages\ListRecords;

class ListFaktaNilais extends ListRecords
{
    protected static string $resource = FaktaNilaiResource::class;

    protected function getHeaderActions(): array
    {
        return [
            Actions\CreateAction::make(),
        ];
    }
}
