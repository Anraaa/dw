<?php

namespace App\Exports;

use App\Models\FaktaBeasiswa;
use Maatwebsite\Excel\Concerns\FromCollection;
use Maatwebsite\Excel\Concerns\WithHeadings;
use Maatwebsite\Excel\Concerns\WithMapping;

class FaktaBeasiswaExport implements FromCollection, WithHeadings, WithMapping
{
    /**
    * @return \Illuminate\Support\Collection
    */
    public function collection()
    {
        return FaktaBeasiswa::with(['mahasiswa', 'beasiswa', 'semester'])->get();
    }

    /**
     * @return array
     */
    public function headings(): array
    {
        return [
            'ID',
            'NIM Mahasiswa',
            'Nama Mahasiswa',
            'Nama Beasiswa',
            'Jenis Beasiswa',
            'Tahun Ajaran',
            'Semester',
            'IPK Saat Terima',
            'SKS Saat Terima',
            'Tanggal Penerimaan',
            'Tanggal Berakhir',
            'Status Pemberian',
            'Sumber Dana',
            'Jumlah Bantuan (Rp)',
            'Dibuat Pada',
            'Diperbarui Pada',
        ];
    }

    /**
     * @var FaktaBeasiswa $faktaBeasiswa
     */
    public function map($faktaBeasiswa): array
    {
        return [
            $faktaBeasiswa->id,
            $faktaBeasiswa->mahasiswa->nim ?? 'N/A',
            $faktaBeasiswa->mahasiswa->nama ?? 'N/A',
            $faktaBeasiswa->beasiswa->nama_beasiswa ?? 'N/A',
            $faktaBeasiswa->beasiswa->jenis_beasiswa ?? 'N/A',
            $faktaBeasiswa->semester->tahun_ajaran ?? 'N/A',
            $faktaBeasiswa->semester->semester ?? 'N/A',
            $faktaBeasiswa->ipk_saat_penerimaan,
            $faktaBeasiswa->sks_saat_penerimaan,
            $faktaBeasiswa->tanggal_penerimaan ? \Carbon\Carbon::parse($faktaBeasiswa->tanggal_penerimaan)->format('d M Y') : 'N/A',
            $faktaBeasiswa->tanggal_berakhir ? \Carbon\Carbon::parse($faktaBeasiswa->tanggal_berakhir)->format('d M Y') : 'N/A',
            $faktaBeasiswa->status_pemberian,
            $faktaBeasiswa->sumber_dana ?? 'N/A',
            $faktaBeasiswa->jumlah_bantuan,
            $faktaBeasiswa->created_at ? \Carbon\Carbon::parse($faktaBeasiswa->created_at)->format('d M Y, H:i') : 'N/A',
            $faktaBeasiswa->updated_at ? \Carbon\Carbon::parse($faktaBeasiswa->updated_at)->format('d M Y, H:i') : 'N/A',
        ];
    }
}