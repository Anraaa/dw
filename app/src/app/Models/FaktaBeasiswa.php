<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Factories\HasFactory;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\BelongsTo;

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

    protected $with = [
        'mahasiswa', // Ensure these relationships exist and are named correctly
        'beasiswa',
        'semester'
    ];

    // Define relationships
    public function mahasiswa(): BelongsTo
    {
        // Parameter 1: Related Model Class
        // Parameter 2: Foreign Key on FaktaBeasiswa table (id_mahasiswa)
        // Parameter 3: Primary Key on DimMahasiswa table (id_mahasiswa)
        return $this->belongsTo(DimMahasiswa::class, 'id_mahasiswa', 'id_mahasiswa');
    }

    public function beasiswa(): BelongsTo
    {
        // Parameter 1: Related Model Class
        // Parameter 2: Foreign Key on FaktaBeasiswa table (id_beasiswa)
        // Parameter 3: Primary Key on DimBeasiswa table (id_beasiswa)
        return $this->belongsTo(DimBeasiswa::class, 'id_beasiswa', 'id_beasiswa');
    }

    public function semester(): BelongsTo
    {
        // Parameter 1: Related Model Class
        // Parameter 2: Foreign Key on FaktaBeasiswa table (id_semester)
        // Parameter 3: Primary Key on DimSemester table (id_semester)
        return $this->belongsTo(DimSemester::class, 'id_semester', 'id_semester');
    }

    // If DimMahasiswa also has a 'prodi' relationship for 'mahasiswa.prodi.nama_prodi',
    // ensure that relationship in DimMahasiswa also correctly specifies its keys:
    // in app/Models/DimMahasiswa.php:
    // public function prodi(): BelongsTo
    // {
    //     return $this->belongsTo(DimProdi::class, 'id_prodi_mahasiswa', 'id_prodi'); // Example
    // }
}