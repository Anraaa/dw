<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Model;

class DimMataKuliah extends Model
{
    protected $table = 'dim_matakuliah';
    protected $primaryKey = 'id_matakuliah';

    protected $fillable = [
        'kode_mk',
        'nama_mk',
        'sks',
        'semester_mk'
    ];
}
