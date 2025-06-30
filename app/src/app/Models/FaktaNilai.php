<?php
namespace App\Models;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Factories\HasFactory; // Tambahkan ini jika belum ada

class FaktaNilai extends Model
{
    use HasFactory; // Gunakan HasFactory

    protected $table = 'fakta_nilai'; // Pastikan nama tabel benar
    protected $fillable = [
        'id_mahasiswa', 'id_matakuliah', 'id_dosen', 'id_semester',
        'nilai_akhir', 'status_kelulusan'
    ];

    // Pastikan relasi ke DimMataKuliah ini ada dan benar (penting untuk perhitungan IPK)
    public function matakuliah()
    {
        return $this->belongsTo(DimMataKuliah::class, 'id_matakuliah', 'id_matakuliah');
    }
    // Pastikan relasi lainnya juga ada: mahasiswa, dosen, semester
    public function mahasiswa() { return $this->belongsTo(DimMahasiswa::class, 'id_mahasiswa', 'id_mahasiswa'); }
    public function dosen() { return $this->belongsTo(DimDosen::class, 'id_dosen', 'id_dosen'); }
    public function semester() { return $this->belongsTo(DimSemester::class, 'id_semester', 'id_semester'); }
}