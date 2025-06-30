<?php

namespace App\Filament\Widgets;

use App\Models\DimMahasiswa;
use Filament\Widgets\BarChartWidget;

class IpkByFacultyChart extends BarChartWidget
{
    protected static ?string $heading = 'Distribusi IPK per Fakultas'; // Set label/heading here

    protected function getData(): array
    {
        return [
            'labels' => DimMahasiswa::select('fakultas')
                ->distinct()
                ->pluck('fakultas')
                ->toArray(),
            'datasets' => [
                [
                    'label' => 'Rata-rata IPK',
                    'data' => DimMahasiswa::select('fakultas')
                        ->selectRaw('avg(fakta_ipk.ipk) as avg_ipk')
                        ->join('fakta_ipk', 'dim_mahasiswa.id_mahasiswa', '=', 'fakta_ipk.id_mahasiswa')
                        ->groupBy('fakultas')
                        ->pluck('avg_ipk')
                        ->map(fn ($value) => round($value, 2))
                        ->toArray(),
                    'backgroundColor' => '#4CAF50',
                ],
            ],
        ];
    }

    protected function getOptions(): array
    {
        return [
            'legend' => ['display' => true],
            'scales' => [
                'y' => ['min' => 0, 'max' => 4],
            ],
        ];
    }
}