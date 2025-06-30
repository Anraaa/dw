<?php

namespace App\Filament\Widgets;

use App\Models\DimMahasiswa;
use Filament\Widgets\PieChartWidget;

class ScholarshipPercentageChart extends PieChartWidget
{
    protected static ?string $heading = 'Persentase Penerima Beasiswa'; // Set label/heading here

    protected function getData(): array
    {
        return [
            'labels' => ['Ya', 'Tidak'],
            'datasets' => [
                [
                    'data' => [
                        DimMahasiswa::where('status_beasiswa', 'Ya')->count(),
                        DimMahasiswa::where('status_beasiswa', 'Tidak')->count(),
                    ],
                    'backgroundColor' => ['#4CAF50', '#F44336'],
                ],
            ],
        ];
    }

    protected function getOptions(): array
    {
        return [
            'legend' => ['display' => true],
        ];
    }
}