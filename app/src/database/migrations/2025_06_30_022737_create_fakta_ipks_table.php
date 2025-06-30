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
        Schema::create('fakta_ipk', function (Blueprint $table) {
            $table->id();
            $table->foreignId('id_mahasiswa')->constrained('dim_mahasiswa', 'id_mahasiswa');
            $table->decimal('total_point', 10, 2);
            $table->integer('total_sks');
            $table->decimal('ipk', 3, 2);
            $table->timestamps();
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::dropIfExists('fakta_ipk');
    }
};
