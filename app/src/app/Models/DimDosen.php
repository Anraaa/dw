<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Model;

class DimDosen extends Model
{
    protected $table = 'dim_dosen';
    protected $primaryKey = 'id_dosen';

    protected $fillable = [
        'nidn',
        'nama_dosen',
        'jabatan',
        'fakultas',
        'gelar_depan',
        'gelar_belakang',
        'email',
        'telepon'
    ];

    public function mataKuliahDiajar()
    {
        return $this->hasMany(FaktaNilai::class, 'id_dosen', 'id_dosen');
    }
}
