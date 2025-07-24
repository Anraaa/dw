<?php

namespace App\Exports;

use App\Models\FaktaBeasiswa;
use Maatwebsite\Excel\Concerns\FromQuery;
use Maatwebsite\Excel\Concerns\WithHeadings;
use Maatwebsite\Excel\Concerns\WithMapping;
use Maatwebsite\Excel\Concerns\ShouldAutoSize;
use Maatwebsite\Excel\Concerns\WithStyles;
use PhpOffice\PhpSpreadsheet\Worksheet\Worksheet;
use Filament\Actions\Exports\ExportColumn;
use Illuminate\Database\Eloquent\Builder;

class FaktaBeasiswaSpecificExport implements FromQuery, WithHeadings, WithMapping, ShouldAutoSize, WithStyles
{
    protected $selectedRows;

    public function __construct($selectedRows = [])
    {
        $this->selectedRows = $selectedRows;
    }

    public static function getColumns(): array
    {
        return [
            ExportColumn::make('mahasiswa.nim')->label('NIM'),
            ExportColumn::make('mahasiswa.nama')->label('Nama Mahasiswa'),
            ExportColumn::make('beasiswa.jenis_beasiswa')->label('Jenis Beasiswa'),
            ExportColumn::make('beasiswa.nama_beasiswa')->label('Nama Beasiswa'),
            ExportColumn::make('semester.tahun_ajaran')->label('Tahun Ajaran'),
            ExportColumn::make('semester.semester')->label('Semester'),
            ExportColumn::make('ipk_saat_penerimaan')->label('IPK Saat Penerimaan'),
            ExportColumn::make('sks_saat_penerimaan')->label('SKS Saat Penerimaan'),
            ExportColumn::make('tanggal_penerimaan')->label('Tanggal Penerimaan'),
            ExportColumn::make('tanggal_berakhir')->label('Tanggal Berakhir'),
            ExportColumn::make('status_pemberian')->label('Status Pemberian'),
            ExportColumn::make('sumber_dana')->label('Sumber Dana'),
            ExportColumn::make('jumlah_bantuan')->label('Jumlah Bantuan (Rp)'),
        ];
    }

    // THIS METHOD MUST BE STATIC TO BE CALLED STATICALLY
    public static function modifyQuery(Builder $query): Builder
    {
        // This method should ideally not be needed here.
        // It's for diagnostic purposes based on your error.
        return $query;
    }

    public function query(): Builder
    {
        $query = FaktaBeasiswa::query()->with(['mahasiswa', 'beasiswa', 'semester']);

        if (!empty($this->selectedRows)) {
            $query->whereIn('id', $this->selectedRows);
        } else {
            $query->whereRaw('1 = 0');
        }

        return $query;
    }

    public function headings(): array
    {
        return [
            'NIM',
            'Nama Mahasiswa',
            'Jenis Beasiswa',
            'Nama Beasiswa',
            'Tahun Ajaran',
            'Semester',
            'IPK Saat Penerimaan',
            'SKS Saat Penerimaan',
            'Tanggal Penerimaan',
            'Tanggal Berakhir',
            'Status Pemberian',
            'Sumber Dana',
            'Jumlah Bantuan (Rp)',
        ];
    }

    public function map($beasiswa): array
    {
        return [
            $beasiswa->mahasiswa->nim ?? 'N/A',
            $beasiswa->mahasiswa->nama ?? 'N/A',
            $beasiswa->beasiswa->jenis_beasiswa ?? 'N/A',
            $beasiswa->beasiswa->nama_beasiswa ?? 'N/A',
            $beasiswa->semester->tahun_ajaran ?? 'N/A',
            $beasiswa->semester->semester ?? 'N/A',
            $beasiswa->ipk_saat_penerimaan ? number_format($beasiswa->ipk_saat_penerimaan, 2) : 'N/A',
            $beasiswa->sks_saat_penerimaan ?? 'N/A',
            $beasiswa->tanggal_penerimaan?->format('d-m-Y') ?? 'N/A',
            $beasiswa->tanggal_berakhir?->format('d-m-Y') ?? 'N/A',
            $beasiswa->status_pemberian ?? 'N/A',
            $beasiswa->sumber_dana ?? 'N/A',
            $beasiswa->jumlah_bantuan ? number_format($beasiswa->jumlah_bantuan, 0, ',', '.') : '0',
        ];
    }

    public function styles(Worksheet $sheet)
    {
        return [
            1 => ['font' => ['bold' => true]],
            'A1:M1' => [
                'fill' => [
                    'fillType' => \PhpOffice\PhpSpreadsheet\Style\Fill::FILL_SOLID,
                    'startColor' => ['argb' => 'FFD9D9D9'],
                ],
            ],
        ];
    }
}