<?php
namespace App\Models;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Factories\HasFactory; // Tambahkan ini jika belum ada

class DimMahasiswa extends Model
{
    use HasFactory; // Gunakan HasFactory

    protected $table = 'dim_mahasiswa';
    protected $primaryKey = 'id_mahasiswa';

    protected $fillable = [
        'nim', 'nama', 'fakultas', 'prodi', 'tahun_masuk', 'status_beasiswa'
    ];

    // Pastikan relasi ke FaktaIpk ini ada dan benar
    public function ipk() // Nama relasi yang digunakan di ETL adalah 'ipk'
    {
        return $this->hasOne(FaktaIpk::class, 'id_mahasiswa', 'id_mahasiswa');
    }

    // Relasi ke FaktaNilai (jika dibutuhkan, tapi tidak langsung di ETL ini)
    public function faktaNilais()
    {
        return $this->hasMany(FaktaNilai::class, 'id_mahasiswa', 'id_mahasiswa');
    }
}