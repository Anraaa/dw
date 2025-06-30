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
        Schema::table('dim_beasiswa', function (Blueprint $table) {
            $table->string('target_fakultas')->nullable()->after('deskripsi');
            $table->string('target_prodi')->nullable()->after('target_fakultas');
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::table('dim_beasiswa', function (Blueprint $table) {
            $table->dropColumn(['target_fakultas', 'target_prodi']);
        });
    }
};
