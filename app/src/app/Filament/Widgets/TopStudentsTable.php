<?php

namespace App\Filament\Widgets;

use App\Models\FaktaIpk;
use Filament\Tables\Table; // Import Table
use Filament\Widgets\TableWidget as BaseTableWidget; // Alias to avoid conflict
use Filament\Tables\Columns\TextColumn; // Import TextColumn

class TopStudentsTable extends BaseTableWidget
{
    protected static ?string $heading = 'Top Mahasiswa Berdasarkan IPK';

    protected function getTableQuery(): \Illuminate\Database\Eloquent\Builder
    {
        return FaktaIpk::query()->orderByDesc('ipk')->limit(10);
    }

    protected function getTableColumns(): array
    {
        return [
            TextColumn::make('mahasiswa.nama')->label('Nama'),
            TextColumn::make('mahasiswa.fakultas')->label('Fakultas'),
            TextColumn::make('ipk')
                ->label('IPK')
                ->formatStateUsing(fn (float $state): string => number_format($state, 2)),
            TextColumn::make('mahasiswa.status_beasiswa')->label('Beasiswa'),
        ];
    }
}