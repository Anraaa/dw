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
        Schema::table('fakta_nilai', function (Blueprint $table) {
            $table->enum('status_kelulusan', ['Lulus', 'Tidak Lulus', 'Mengulang'])->change();
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::table('fakta_nilai', function (Blueprint $table) {
            $table->enum('status_kelulusan', ['Lulus', 'Tidak Lulus', 'Mengulang'])->change();
        });
    }
};
