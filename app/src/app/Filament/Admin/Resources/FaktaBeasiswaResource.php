<?php

namespace App\Filament\Admin\Resources;

use App\Filament\Admin\Resources\FaktaBeasiswaResource\Pages;
use App\Models\FaktaBeasiswa;
use App\Models\DimMahasiswa;   // Import DimMahasiswa for relationships
use App\Models\DimBeasiswa;    // Import DimBeasiswa for relationships
use App\Models\DimSemester;   // Import DimSemester for relationships
use Filament\Forms;
use Filament\Forms\Form;
use Filament\Resources\Resource;
use Filament\Tables;
use Filament\Tables\Table;
use Illuminate\Database\Eloquent\Builder;
use Illuminate\Database\Eloquent\SoftDeletingScope;
use Filament\Tables\Filters\SelectFilter; // Ensure this is imported
use Filament\Tables\Filters\Filter;       // Ensure this is imported

class FaktaBeasiswaResource extends Resource
{
    protected static ?string $model = FaktaBeasiswa::class;

    protected static ?string $navigationIcon = 'heroicon-o-currency-dollar'; // Icon for financial/award data
    protected static ?string $modelLabel = 'Alokasi Beasiswa'; // Custom singular label
    protected static ?string $pluralModelLabel = 'Alokasi Beasiswa'; // Custom plural label
    protected static ?string $navigationLabel = 'Alokasi Beasiswa'; // Custom navigation label
    protected static ?string $navigationGroup = 'Data Akademik'; // Group with other academic data
    protected static ?int $navigationSort = 7; // Order within the group

    public static function form(Form $form): Form
    {
        return $form
            ->schema([
                Forms\Components\Section::make('Detail Alokasi Beasiswa')
                    ->description('Catatan rinci tentang beasiswa yang diberikan kepada mahasiswa.')
                    ->schema([
                        Forms\Components\Select::make('id_mahasiswa')
                            ->label('Mahasiswa Penerima')
                            ->relationship('mahasiswa', 'nama') // Assumes 'mahasiswa' relationship in FaktaBeasiswa model
                            ->getOptionLabelUsing(fn (DimMahasiswa $record) => "{$record->nim} - {$record->nama}")
                            ->searchable()
                            ->preload()
                            ->required(),

                        Forms\Components\Select::make('id_beasiswa')
                            ->label('Jenis Beasiswa')
                            ->relationship('beasiswa', 'nama_beasiswa') // Assumes 'beasiswa' relationship in FaktaBeasiswa model
                            ->getOptionLabelUsing(fn (DimBeasiswa $record) => "{$record->nama_beasiswa} ({$record->jenis_beasiswa})")
                            ->searchable()
                            ->preload()
                            ->required(),

                        Forms\Components\Select::make('id_semester')
                            ->label('Semester Pemberian')
                            ->relationship('semester', 'tahun_ajaran') // Assumes 'semester' relationship in FaktaBeasiswa model
                            ->getOptionLabelUsing(fn (DimSemester $record) => "{$record->tahun_ajaran} - {$record->semester}")
                            ->searchable()
                            ->preload()
                            ->nullable(), // Nullable as per migration

                        Forms\Components\TextInput::make('ipk_saat_penerimaan')
                            ->label('IPK Saat Penerimaan')
                            ->required()
                            ->numeric()
                            ->minValue(0.00)
                            ->maxValue(4.00)
                            ->step(0.01)
                            ->placeholder('Contoh: 3.85')
                            ->suffix('IPK'),

                        Forms\Components\TextInput::make('sks_saat_penerimaan')
                            ->label('SKS Saat Penerimaan')
                            ->required()
                            ->numeric()
                            ->minValue(0)
                            ->placeholder('Contoh: 70')
                            ->suffix('SKS'),

                        Forms\Components\DatePicker::make('tanggal_penerimaan')
                            ->label('Tanggal Penerimaan')
                            ->required()
                            ->native(false)
                            ->closeOnDateSelection(),

                        Forms\Components\DatePicker::make('tanggal_berakhir')
                            ->label('Tanggal Berakhir Beasiswa')
                            ->native(false)
                            ->closeOnDateSelection()
                            ->nullable(),

                        Forms\Components\Select::make('status_pemberian')
                            ->label('Status Pemberian')
                            ->required()
                            ->options([
                                'Aktif' => 'Aktif',
                                'Non-Aktif' => 'Non-Aktif',
                                'Dicabut' => 'Dicabut',
                                'Selesai' => 'Selesai',
                            ])
                            ->default('Aktif')
                            ->native(false),

                        Forms\Components\TextInput::make('sumber_dana')
                            ->label('Sumber Dana')
                            ->maxLength(255)
                            ->placeholder('Contoh: Kementerian Pendidikan')
                            ->nullable(),

                        Forms\Components\TextInput::make('jumlah_bantuan')
                            ->label('Jumlah Bantuan (Rp)')
                            ->numeric()
                            ->minValue(0)
                            ->suffix('Rp') // Tambahkan suffix Rupiah
                            ->placeholder('Contoh: 2500000.00')
                            ->nullable(),
                    ])
                    ->columns(2), // Arrange fields in 2 columns
            ]);
    }

    public static function table(Table $table): Table
    {
        return $table
            ->columns([
                Tables\Columns\TextColumn::make('mahasiswa.nama') // Display Mahasiswa's Name
                    ->label('Mahasiswa')
                    ->searchable()
                    ->sortable(),
                Tables\Columns\TextColumn::make('beasiswa.nama_beasiswa') // Display Beasiswa Name
                    ->label('Nama Beasiswa')
                    ->searchable()
                    ->sortable(),
                Tables\Columns\TextColumn::make('semester.tahun_ajaran') // Display Semester TA
                    ->label('Tahun Ajaran')
                    ->sortable(),
                Tables\Columns\TextColumn::make('semester.semester') // Display Semester Name
                    ->label('Semester')
                    ->sortable(),
                Tables\Columns\TextColumn::make('ipk_saat_penerimaan')
                    ->label('IPK Saat Terima')
                    ->numeric()
                    ->sortable()
                    ->badge()
                    ->color(fn (float $state): string => match (true) {
                        $state >= 3.50 => 'success',
                        $state >= 3.00 => 'info',
                        default => 'warning',
                    }),
                Tables\Columns\TextColumn::make('sks_saat_penerimaan')
                    ->label('SKS Saat Terima')
                    ->numeric()
                    ->sortable()
                    ->suffix(' SKS'),
                Tables\Columns\TextColumn::make('tanggal_penerimaan')
                    ->label('Tanggal Terima')
                    ->date('d M Y') // Format date
                    ->sortable(),
                Tables\Columns\TextColumn::make('tanggal_berakhir')
                    ->label('Tanggal Berakhir')
                    ->date('d M Y')
                    ->sortable()
                    ->placeholder('N/A'), // Tampilkan N/A jika null
                Tables\Columns\TextColumn::make('status_pemberian')
                    ->label('Status')
                    ->badge() // Tampilkan sebagai badge
                    ->color(fn (string $state): string => match ($state) {
                        'Aktif' => 'success',
                        'Selesai' => 'gray',
                        'Non-Aktif' => 'warning',
                        'Dicabut' => 'danger',
                        default => 'secondary',
                    })
                    ->searchable()
                    ->sortable(),
                Tables\Columns\TextColumn::make('jumlah_bantuan')
                    ->label('Jumlah Bantuan')
                    ->numeric()
                    ->sortable()
                    ->money('IDR', locale: 'id') // Format sebagai mata uang Rupiah
                    ->placeholder('Tidak Ada'), // Tampilkan jika null
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
            ->defaultSort('tanggal_penerimaan', 'desc') // Default sort by most recent allocation
            ->filters([
                Tables\Filters\SelectFilter::make('id_mahasiswa')
                    ->label('Filter per Mahasiswa')
                    ->relationship('mahasiswa', 'nama')
                    ->searchable()
                    ->preload(),
                Tables\Filters\SelectFilter::make('id_beasiswa')
                    ->label('Filter per Beasiswa')
                    ->relationship('beasiswa', 'nama_beasiswa')
                    ->searchable()
                    ->preload(),
                Tables\Filters\SelectFilter::make('id_semester')
                    ->label('Filter per Semester')
                    ->relationship('semester', 'tahun_ajaran')
                    ->getOptionLabelUsing(fn (DimSemester $record) => "{$record->tahun_ajaran} - {$record->semester}")
                    ->searchable()
                    ->preload(),
                Tables\Filters\SelectFilter::make('status_pemberian')
                    ->label('Filter per Status')
                    ->options([
                        'Aktif' => 'Aktif',
                        'Non-Aktif' => 'Non-Aktif',
                        'Dicabut' => 'Dicabut',
                        'Selesai' => 'Selesai',
                    ]),
                Tables\Filters\Filter::make('ipk_criteria')
                    ->label('IPK')
                    ->form([
                        Forms\Components\TextInput::make('min_ipk')
                            ->numeric()
                            ->step(0.01)
                            ->placeholder('Min IPK'),
                        Forms\Components\TextInput::make('max_ipk')
                            ->numeric()
                            ->step(0.01)
                            ->placeholder('Max IPK'),
                    ])
                    ->query(function (Builder $query, array $data): Builder {
                        return $query
                            ->when(
                                $data['min_ipk'],
                                fn (Builder $query, $ipk): Builder => $query->where('ipk_saat_penerimaan', '>=', $ipk),
                            )
                            ->when(
                                $data['max_ipk'],
                                fn (Builder $query, $ipk): Builder => $query->where('ipk_saat_penerimaan', '<=', $ipk),
                            );
                    }),
                Tables\Filters\Filter::make('tanggal_penerimaan_range')
                    ->label('Rentang Tanggal Penerimaan')
                    ->form([
                        Forms\Components\DatePicker::make('from_date')
                            ->label('Dari Tanggal')
                            ->native(false),
                        Forms\Components\DatePicker::make('to_date')
                            ->label('Sampai Tanggal')
                            ->native(false),
                    ])
                    ->query(function (Builder $query, array $data): Builder {
                        return $query
                            ->when(
                                $data['from_date'],
                                fn (Builder $query, $date): Builder => $query->whereDate('tanggal_penerimaan', '>=', $date),
                            )
                            ->when(
                                $data['to_date'],
                                fn (Builder $query, $date): Builder => $query->whereDate('tanggal_penerimaan', '<=', $date),
                            );
                    }),
                // Tables\Filters\TrashedFilter::make(), // Uncomment if you use Soft Deletes
            ])
            ->actions([
                Tables\Actions\ActionGroup::make([
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
            ->groups([ // Grouping options for the table
                Tables\Grouping\Group::make('beasiswa.nama_beasiswa')
                    ->label('Berdasarkan Beasiswa')
                    ->collapsible(),
                Tables\Grouping\Group::make('status_pemberian')
                    ->label('Status Pemberian')
                    ->collapsible(),
                Tables\Grouping\Group::make('semester.tahun_ajaran')
                    ->label('Tahun Ajaran')
                    ->collapsible(),
            ]);
    }

    public static function getRelations(): array
    {
        return [
            // No direct relations manager needed for a fact table unless you want to see specific dimensions
        ];
    }

    public static function getPages(): array
    {
        return [
            'index' => Pages\ListFaktaBeasiswas::route('/'),
            'create' => Pages\CreateFaktaBeasiswa::route('/create'),
            'edit' => Pages\EditFaktaBeasiswa::route('/{record}/edit'),
        ];
    }

    public static function getNavigationBadge(): ?string
    {
        return static::getModel()::count();
    }

    public static function getNavigationBadgeColor(): ?string
    {
        return static::getModel()::where('status_pemberian', 'Aktif')->count() > 0 ? 'success' : 'gray'; // Badge hijau jika ada alokasi aktif
    }
}