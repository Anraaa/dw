<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Model;

class FaktaIpk extends Model
{
    protected $table = 'fakta_ipk';

    protected $fillable = [
        'id_mahasiswa',
        'total_point',
        'total_sks',
        'ipk'
    ];

    public function mahasiswa()
    {
        return $this->belongsTo(DimMahasiswa::class, 'id_mahasiswa');
    }
}
