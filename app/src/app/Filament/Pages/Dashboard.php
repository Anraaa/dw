<?php

namespace App\Filament\Pages;

use Filament\Pages\Page;
use App\Filament\Widgets\StudentStatsOverview;
use App\Filament\Widgets\IpkByFacultyChart;
use App\Filament\Widgets\ScholarshipPercentageChart;
use App\Filament\Widgets\TopStudentsTable;
use Filament\Pages\Concerns\InteractsWithHeaderWidgets;

class Dashboard extends Page
{
    
    protected static ?string $navigationIcon = 'heroicon-o-document-text';
    protected static string $view = 'filament.pages.dashboard';

    protected function getHeaderWidgets(): array
    {
        return [
            StudentStatsOverview::class,
            IpkByFacultyChart::class,
            ScholarshipPercentageChart::class,
            TopStudentsTable::class,
        ];
    }
}
