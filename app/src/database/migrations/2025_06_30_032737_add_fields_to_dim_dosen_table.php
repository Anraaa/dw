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
        Schema::table('dim_dosen', function (Blueprint $table) {
            $table->string('gelar_depan', 20)->nullable();
            $table->string('gelar_belakang', 20)->nullable();
            $table->string('email', 100)->nullable();
            $table->string('telepon', 20)->nullable();
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::table('dim_dosen', function (Blueprint $table) {
            //
        });
    }
};
