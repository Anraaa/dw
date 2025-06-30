<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Factories\HasFactory;
use Illuminate\Database\Eloquent\Model;

class DimBeasiswa extends Model
{
    use HasFactory;

    protected $table = 'dim_beasiswa'; // Nama tabel yang sesuai
    protected $primaryKey = 'id_beasiswa'; // Definisikan primary key

    protected $fillable = [
        'nama_beasiswa',
        'jenis_beasiswa',
        'min_ipk_kriteria',
        'min_sks_kriteria',
        'kapasitas_slot',
        'tanggal_mulai_pendaftaran',
        'tanggal_tutup_pendaftaran',
        'is_aktif',
        'deskripsi',
        'target_fakultas', // Tambahkan ini
        'target_prodi',
    ];

    protected $casts = [
        'is_aktif' => 'boolean',
        'tanggal_mulai_pendaftaran' => 'date',
        'tanggal_tutup_pendaftaran' => 'date',
    ];

    // Jika Anda ingin melihat mahasiswa yang menerima beasiswa ini
    public function faktaBeasiswas()
    {
        return $this->hasMany(FaktaBeasiswa::class, 'id_beasiswa', 'id_beasiswa');
    }
}
