<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Factories\HasFactory;
use Illuminate\Database\Eloquent\Model;

class FaktaBeasiswa extends Model
{
    use HasFactory;

    protected $table = 'fakta_beasiswa';

    protected $fillable = [
        'id_mahasiswa',
        'id_beasiswa',
        'id_semester',
        'ipk_saat_penerimaan',
        'sks_saat_penerimaan',
        'tanggal_penerimaan',
        'tanggal_berakhir',
        'status_pemberian',
        'sumber_dana',
        'jumlah_bantuan',
    ];

    protected $casts = [
        'tanggal_penerimaan' => 'date',
        'tanggal_berakhir' => 'date',
    ];

    // Relasi ke dimensi-dimensi terkait
    public function mahasiswa()
    {
        return $this->belongsTo(DimMahasiswa::class, 'id_mahasiswa', 'id_mahasiswa');
    }

    public function beasiswa()
    {
        return $this->belongsTo(DimBeasiswa::class, 'id_beasiswa', 'id_beasiswa');
    }

    public function semester()
    {
        return $this->belongsTo(DimSemester::class, 'id_semester', 'id_semester');
    }
}