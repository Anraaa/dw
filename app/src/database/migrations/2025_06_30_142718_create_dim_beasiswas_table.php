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
        Schema::create('dim_beasiswa', function (Blueprint $table) {
            $table->id('id_beasiswa'); // Primary key khusus untuk dimensi beasiswa
            $table->string('nama_beasiswa')->unique();
            $table->string('jenis_beasiswa')->nullable(); // Misal: 'Prestasi', 'Bantuan UKT', 'Internal', 'Eksternal'
            $table->decimal('min_ipk_kriteria', 3, 2)->default(0.00); // Kriteria IPK minimal
            $table->integer('min_sks_kriteria')->default(0); // Kriteria SKS minimal
            $table->integer('kapasitas_slot')->default(0); // Jumlah slot beasiswa yang tersedia (0=tak terbatas)
            $table->date('tanggal_mulai_pendaftaran')->nullable();
            $table->date('tanggal_tutup_pendaftaran')->nullable();
            $table->boolean('is_aktif')->default(true); // Status beasiswa aktif/tidak aktif
            $table->text('deskripsi')->nullable();
            $table->timestamps();
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::dropIfExists('dim_beasiswas');
    }
};
