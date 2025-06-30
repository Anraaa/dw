<?php

namespace App\Filament\Admin\Resources;

use App\Filament\Admin\Resources\DimBeasiswaResource\Pages;
use App\Models\DimBeasiswa;
use Filament\Forms;
use Filament\Forms\Form;
use Filament\Resources\Resource;
use Filament\Tables;
use Filament\Tables\Table;
use Illuminate\Database\Eloquent\Builder;
use Illuminate\Database\Eloquent\SoftDeletingScope;
use Filament\Tables\Filters\SelectFilter; // Pastikan ini ada
use Filament\Tables\Filters\Filter;       // Pastikan ini ada

class DimBeasiswaResource extends Resource
{
    protected static ?string $model = DimBeasiswa::class;

    protected static ?string $navigationIcon = 'heroicon-o-gift'; // Icon yang lebih relevan untuk beasiswa
    protected static ?string $modelLabel = 'Jenis Beasiswa'; // Label singular kustom
    protected static ?string $pluralModelLabel = 'Jenis Beasiswa'; // Label plural kustom
    protected static ?string $navigationLabel = 'Data Beasiswa'; // Label navigasi kustom
    protected static ?string $navigationGroup = 'Data Akademik'; // Kelompokkan dengan data akademik lain
    protected static ?int $navigationSort = 5; // Urutan dalam grup (setelah IPK)

    public static function form(Form $form): Form
    {
        return $form
            ->schema([
                Forms\Components\Section::make('Informasi Dasar Beasiswa')
                    ->description('Detail umum dan kriteria kelayakan beasiswa.')
                    ->schema([
                        Forms\Components\TextInput::make('nama_beasiswa')
                            ->label('Nama Beasiswa')
                            ->required()
                            ->unique(ignoreRecord: true) // Pastikan nama beasiswa unik
                            ->maxLength(255)
                            ->placeholder('Contoh: Beasiswa Unggulan Nasional'),

                        Forms\Components\Select::make('jenis_beasiswa')
                            ->label('Jenis Beasiswa')
                            ->options([
                                'Prestasi' => 'Prestasi Akademik/Non-Akademik',
                                'Bantuan Ekonomi' => 'Bantuan Ekonomi',
                                'Internal' => 'Internal Universitas',
                                'Eksternal' => 'Eksternal (Pemerintah/Swasta)',
                                'Lainnya' => 'Lainnya',
                            ])
                            ->native(false) // Gunakan styling Filament
                            ->searchable() // Bisa dicari di dropdown
                            ->nullable(),

                        Forms\Components\TextInput::make('min_ipk_kriteria')
                            ->label('IPK Minimum')
                            ->required()
                            ->numeric()
                            ->minValue(0.00)
                            ->maxValue(4.00)
                            ->step(0.01) // Izinkan dua desimal
                            ->placeholder('Contoh: 3.50')
                            ->suffix('IPK'), // Tambahkan suffix

                        Forms\Components\TextInput::make('min_sks_kriteria')
                            ->label('SKS Minimum')
                            ->required()
                            ->numeric()
                            ->minValue(0)
                            ->placeholder('Contoh: 60')
                            ->suffix('SKS'), // Tambahkan suffix

                        Forms\Components\TextInput::make('kapasitas_slot')
                            ->label('Kapasitas Slot')
                            ->helperText('Isi 0 jika kapasitas tidak terbatas.') // Bantuan teks
                            ->required()
                            ->numeric()
                            ->minValue(0)
                            ->placeholder('Contoh: 20'),

                        Forms\Components\DatePicker::make('tanggal_mulai_pendaftaran')
                            ->label('Tanggal Mulai Pendaftaran')
                            ->native(false)
                            ->closeOnDateSelection()
                            ->nullable(),

                        Forms\Components\DatePicker::make('tanggal_tutup_pendaftaran')
                            ->label('Tanggal Tutup Pendaftaran')
                            ->native(false)
                            ->closeOnDateSelection()
                            ->nullable(),

                        Forms\Components\Toggle::make('is_aktif')
                            ->label('Beasiswa Aktif')
                            ->inline(false) // Tampilkan toggle di baris baru
                            ->default(true) // Default aktif
                            ->required(),
                    ])
                    ->columns(2), // Atur field dalam 2 kolom

                Forms\Components\Section::make('Deskripsi Beasiswa')
                    ->collapsible() // Bisa dilipat
                    ->schema([
                        Forms\Components\Textarea::make('deskripsi')
                            ->label('Deskripsi Lengkap')
                            ->rows(5) // Tambah tinggi textarea
                            ->maxLength(65535) // Maksimal Text field di MySQL
                            ->columnSpanFull(),
                    ])
            ]);
    }

    public static function table(Table $table): Table
    {
        return $table
            ->columns([
                Tables\Columns\TextColumn::make('nama_beasiswa')
                    ->label('Nama Beasiswa')
                    ->searchable()
                    ->sortable(),
                Tables\Columns\TextColumn::make('jenis_beasiswa')
                    ->label('Jenis Beasiswa')
                    ->badge() // Tampilkan sebagai badge
                    ->color(fn (string $state): string => match ($state) {
                        'Prestasi' => 'success',
                        'Bantuan Ekonomi' => 'info',
                        'Internal' => 'primary',
                        'Eksternal' => 'warning',
                        default => 'secondary',
                    })
                    ->searchable()
                    ->sortable(),
                Tables\Columns\TextColumn::make('min_ipk_kriteria')
                    ->label('Min IPK')
                    ->numeric()
                    ->sortable()
                    ->suffix(' IPK'),
                Tables\Columns\TextColumn::make('min_sks_kriteria')
                    ->label('Min SKS')
                    ->numeric()
                    ->sortable()
                    ->suffix(' SKS'),
                Tables\Columns\TextColumn::make('kapasitas_slot')
                    ->label('Kapasitas')
                    ->numeric()
                    ->sortable()
                    ->formatStateUsing(fn (int $state): string => $state === 0 ? 'Tak Terbatas' : (string) $state), // Tampilkan 'Tak Terbatas' jika 0
                Tables\Columns\TextColumn::make('tanggal_mulai_pendaftaran')
                    ->label('Mulai Pendaftaran')
                    ->date('d M Y') // Format tanggal
                    ->sortable(),
                Tables\Columns\TextColumn::make('tanggal_tutup_pendaftaran')
                    ->label('Tutup Pendaftaran')
                    ->date('d M Y')
                    ->sortable(),
                Tables\Columns\IconColumn::make('is_aktif')
                    ->label('Aktif')
                    ->boolean()
                    ->trueIcon('heroicon-o-check-circle')
                    ->falseIcon('heroicon-o-x-circle')
                    ->trueColor('success')
                    ->falseColor('danger')
                    ->sortable(),
                Tables\Columns\TextColumn::make('created_at')
                    ->label('Dibuat')
                    ->dateTime('d M Y, H:i')
                    ->sortable()
                    ->toggleable(isToggledHiddenByDefault: true),
                Tables\Columns\TextColumn::make('updated_at')
                    ->label('Diperbarui')
                    ->dateTime('d M Y, H:i')
                    ->sortable()
                    ->toggleable(isToggledHiddenByDefault: true),
            ])
            ->defaultSort('nama_beasiswa', 'asc') // Urutkan berdasarkan nama beasiswa secara default
            ->filters([
                Tables\Filters\SelectFilter::make('jenis_beasiswa')
                    ->label('Filter per Jenis')
                    ->options([
                        'Prestasi' => 'Prestasi',
                        'Bantuan Ekonomi' => 'Bantuan Ekonomi',
                        'Internal' => 'Internal',
                        'Eksternal' => 'Eksternal',
                        'Lainnya' => 'Lainnya',
                    ]),
                Tables\Filters\TernaryFilter::make('is_aktif') // Filter aktif/tidak aktif
                    ->label('Status Aktif')
                    ->boolean()
                    ->trueLabel('Aktif')
                    ->falseLabel('Tidak Aktif')
                    ->nullable(), // Izinkan untuk melihat semua status
                Tables\Filters\Filter::make('kapasitas_slot_status') // Filter untuk kapasitas slot
                    ->label('Kapasitas Slot')
                    ->form([
                        Forms\Components\Radio::make('slot_status')
                            ->options([
                                'limited' => 'Terbatas (>0)',
                                'unlimited' => 'Tak Terbatas (0)',
                            ])
                            ->columns(1) // Tampilkan opsi satu per satu
                    ])
                    ->query(function (Builder $query, array $data): Builder {
                        if ($data['slot_status'] === 'limited') {
                            return $query->where('kapasitas_slot', '>', 0);
                        }
                        if ($data['slot_status'] === 'unlimited') {
                            return $query->where('kapasitas_slot', 0);
                        }
                        return $query; // Tampilkan semua jika tidak ada pilihan
                    }),
            ])
            ->actions([
                Tables\Actions\ActionGroup::make([ // Group actions
                    Tables\Actions\ViewAction::make(),
                    Tables\Actions\EditAction::make(),
                    Tables\Actions\DeleteAction::make(),
                ])->tooltip('Aksi'),
            ])
            ->bulkActions([
                Tables\Actions\BulkActionGroup::make([
                    Tables\Actions\DeleteBulkAction::make(),
                ]),
            ])
            ->emptyStateActions([
                Tables\Actions\CreateAction::make(),
            ])
            ->groups([ // Tambahkan grouping
                Tables\Grouping\Group::make('jenis_beasiswa')
                    ->label('Jenis Beasiswa')
                    ->collapsible(),
                Tables\Grouping\Group::make('is_aktif')
                    ->label('Status Aktif')
                    ->collapsible()
                    ->getTitleFromRecordUsing(fn (DimBeasiswa $record) => $record->is_aktif ? 'Aktif' : 'Tidak Aktif'),
            ]);
    }

    public static function getRelations(): array
    {
        return [
            // RelationManagers\FaktaBeasiswasRelationManager::class, // Jika ada
        ];
    }

    public static function getPages(): array
    {
        return [
            'index' => Pages\ListDimBeasiswas::route('/'),
            'create' => Pages\CreateDimBeasiswa::route('/create'),
            'edit' => Pages\EditDimBeasiswa::route('/{record}/edit'),
        ];
    }

    public static function getNavigationBadge(): ?string
    {
        return static::getModel()::count();
    }

    public static function getNavigationBadgeColor(): ?string
    {
        return static::getModel()::where('is_aktif', true)->count() > 0 ? 'success' : 'gray'; // Badge warna hijau jika ada beasiswa aktif
    }
}