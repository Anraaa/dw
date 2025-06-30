<?php

namespace App\Filament\Widgets;

use App\Models\DimMahasiswa;
use App\Models\FaktaIpk;
use Filament\Widgets\StatsOverviewWidget as BaseStatsOverviewWidget;
use Filament\Widgets\StatsOverviewWidget\Stat; // Ensure this is imported

class StudentStatsOverview extends BaseStatsOverviewWidget
{
    protected function getStats(): array
    {
        return [
            Stat::make('Total Mahasiswa', DimMahasiswa::count()),
            Stat::make('Rata-rata IPK', number_format(FaktaIpk::avg('ipk') ?? 0, 2)),
            Stat::make('Penerima Beasiswa', DimMahasiswa::where('status_beasiswa', 'Ya')->count()),
        ];
    }
}