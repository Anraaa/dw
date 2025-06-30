<?php

namespace App\Filament\Pages;

use Filament\Pages\Page;
use App\Models\FaktaIpk;
use App\Models\DimMahasiswa;
use Filament\Widgets\StatsOverviewWidget;
use Filament\Widgets\StatsOverviewWidget\Stat;
use Filament\Widgets\TableWidget;
use Filament\Widgets\BarChartWidget;
use Filament\Widgets\PieChartWidget;
use Flowframe\Trend\Trend;
use Flowframe\Trend\TrendValue;

class Dashboard extends Page
{
    protected static ?string $navigationIcon = 'heroicon-o-document-text';
    protected static string $view = 'filament.pages.dashboard';
    
    protected function getHeaderWidgets(): array
    {
        return [
            StatsOverviewWidget::make([
                Stat::make('Total Mahasiswa', DimMahasiswa::count()),
                Stat::make('Rata-rata IPK', number_format(FaktaIpk::avg('ipk') ?? 0, 2)),
                Stat::make('Penerima Beasiswa', DimMahasiswa::where('status_beasiswa', 'Ya')->count()),
            ]),
            
            BarChartWidget::make()
                ->label('Distribusi IPK per Fakultas')
                ->options([
                    'legend' => ['display' => true],
                    'scales' => [
                        'y' => ['min' => 0, 'max' => 4],
                    ],
                ])
                ->data([
                    'labels' => DimMahasiswa::select('fakultas')
                        ->distinct()
                        ->pluck('fakultas')
                        ->toArray(),
                    'datasets' => [
                        [
                            'label' => 'Rata-rata IPK',
                            'data' => DimMahasiswa::select('fakultas')
                                ->selectRaw('avg(fakta_ipks.ipk) as avg_ipk')
                                ->join('fakta_ipks', 'dim_mahasiswas.id_mahasiswa', '=', 'fakta_ipks.id_mahasiswa')
                                ->groupBy('fakultas')
                                ->pluck('avg_ipk')
                                ->map(fn ($value) => round($value, 2))
                                ->toArray(),
                            'backgroundColor' => '#4CAF50',
                        ],
                    ],
                ]),
                
            PieChartWidget::make()
                ->label('Persentase Penerima Beasiswa')
                ->options([
                    'legend' => ['display' => true],
                ])
                ->data([
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
                ]),
                
            TableWidget::make()
                ->label('Top Mahasiswa Berdasarkan IPK')
                ->data(function () {
                    return FaktaIpk::with('mahasiswa')
                        ->orderByDesc('ipk')
                        ->limit(10)
                        ->get()
                        ->map(function ($ipk) {
                            return [
                                'nama' => $ipk->mahasiswa->nama,
                                'fakultas' => $ipk->mahasiswa->fakultas,
                                'ipk' => number_format($ipk->ipk, 2),
                                'beasiswa' => $ipk->mahasiswa->status_beasiswa,
                            ];
                        });
                })
                ->columns([
                    'nama' => 'Nama',
                    'fakultas' => 'Fakultas',
                    'ipk' => 'IPK',
                    'beasiswa' => 'Beasiswa',
                ]),
        ];
    }
}