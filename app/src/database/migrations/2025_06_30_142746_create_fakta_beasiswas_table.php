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
        Schema::create('fakta_beasiswa', function (Blueprint $table) {
            $table->id();
            $table->foreignId('id_mahasiswa')->constrained('dim_mahasiswa', 'id_mahasiswa');
            $table->foreignId('id_beasiswa')->constrained('dim_beasiswa', 'id_beasiswa');
            $table->foreignId('id_semester')->nullable()->constrained('dim_semester', 'id_semester');

            $table->decimal('ipk_saat_penerimaan', 3, 2);
            $table->integer('sks_saat_penerimaan');
            $table->date('tanggal_penerimaan');
            $table->date('tanggal_berakhir')->nullable();
            $table->string('status_pemberian', 20)->default('Aktif');
            $table->string('sumber_dana')->nullable();
            $table->decimal('jumlah_bantuan', 10, 2)->nullable();

            $table->timestamps();
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::dropIfExists('fakta_beasiswas');
    }
};
