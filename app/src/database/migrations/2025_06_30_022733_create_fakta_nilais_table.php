<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    /**
     * Run the migrations.
     */
    public function up(): void
    {
        Schema::create('fakta_nilai', function (Blueprint $table) {
            $table->id();
            $table->foreignId('id_mahasiswa')->constrained('dim_mahasiswa', 'id_mahasiswa');
            $table->foreignId('id_matakuliah')->constrained('dim_matakuliah', 'id_matakuliah');
            $table->foreignId('id_dosen')->constrained('dim_dosen', 'id_dosen');
            $table->foreignId('id_semester')->constrained('dim_semester', 'id_semester');
            $table->decimal('nilai_akhir', 5, 2);
            $table->enum('status_kelulusan', ['Lulus', 'Tidak Lulus']);
            $table->timestamps();
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::dropIfExists('fakta_nilai');
    }
};
