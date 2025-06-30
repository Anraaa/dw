<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Model;

class DimSemester extends Model
{
    protected $table = 'dim_semester';
    protected $primaryKey = 'id_semester';

    protected $fillable = [
        'tahun_ajaran',
        'semester'
    ];
}
