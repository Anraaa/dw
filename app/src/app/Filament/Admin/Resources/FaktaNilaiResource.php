<?php

namespace App\Filament\Admin\Resources;

use App\Filament\Admin\Resources\FaktaNilaiResource\Pages;
use App\Models\FaktaNilai;
use App\Models\DimMahasiswa; // Import DimMahasiswa for relationships
use App\Models\DimMataKuliah; // Import DimMataKuliah for relationships
use App\Models\DimDosen;     // Import DimDosen for relationships
use App\Models\DimSemester;  // Import DimSemester for relationships
use Filament\Forms;
use Filament\Forms\Form;
use Filament\Resources\Resource;
use Filament\Tables;
use Filament\Tables\Table;
use Illuminate\Database\Eloquent\Builder;
use Illuminate\Database\Eloquent\SoftDeletingScope;
use Filament\Tables\Filters\SelectFilter; // Make sure this is imported
use Filament\Tables\Filters\Filter;       // Make sure this is imported

class FaktaNilaiResource extends Resource
{
    protected static ?string $model = FaktaNilai::class;

    protected static ?string $navigationIcon = 'heroicon-o-pencil-square'; // Icon for grades/scores
    protected static ?string $modelLabel = 'Data Nilai';
    protected static ?string $pluralModelLabel = 'Data Nilai';
    protected static ?string $navigationLabel = 'Data Nilai';
    protected static ?string $navigationGroup = 'Data Akademik';
    protected static ?int $navigationSort = 6; // Order within the group

    public static function form(Form $form): Form
    {
        return $form
            ->schema([
                Forms\Components\Section::make('Informasi Detail Nilai')
                    ->description('Catatan nilai akhir untuk mahasiswa pada mata kuliah tertentu')
                    ->schema([
                        Forms\Components\Select::make('id_mahasiswa')
                            ->label('Mahasiswa')
                            ->relationship('mahasiswa', 'nama') // Assumes 'mahasiswa' relationship in FaktaNilai
                            ->getOptionLabelUsing(fn (DimMahasiswa $record) => "{$record->nim} - {$record->nama}") // Display NIM - Nama
                            ->searchable()
                            ->preload()
                            ->required(),

                        Forms\Components\Select::make('id_matakuliah')
                            ->label('Mata Kuliah')
                            ->relationship('matakuliah', 'nama_mk') // Assumes 'matakuliah' relationship in FaktaNilai
                            ->getOptionLabelUsing(fn (DimMataKuliah $record) => "{$record->kode_mk} - {$record->nama_mk}") // Display Kode - Nama MK
                            ->searchable()
                            ->preload()
                            ->required(),

                        Forms\Components\Select::make('id_dosen')
                            ->label('Dosen Pengampu')
                            ->relationship('dosen', 'nama_dosen') // Assumes 'dosen' relationship in FaktaNilai
                            ->getOptionLabelUsing(fn (DimDosen $record) => "{$record->nidn} - {$record->nama_dosen}") // Display NIDN - Nama Dosen
                            ->searchable()
                            ->preload()
                            ->required(),

                        Forms\Components\Select::make('id_semester')
                            ->label('Semester')
                            ->relationship('semester', 'tahun_ajaran') // Assumes 'semester' relationship in FaktaNilai
                            ->getOptionLabelUsing(fn (DimSemester $record) => "{$record->tahun_ajaran} - {$record->semester}") // Display Tahun Ajaran - Semester
                            ->searchable()
                            ->preload()
                            ->required(),

                        Forms\Components\TextInput::make('nilai_akhir')
                            ->label('Nilai Akhir')
                            ->required()
                            ->numeric()
                            ->minValue(0)
                            ->maxValue(100) // Assuming a 0-100 scale for grades
                            ->placeholder('Contoh: 85.5'),

                        Forms\Components\Select::make('status_kelulusan')
                            ->label('Status Kelulusan')
                            ->required()
                            ->options([
                                'Lulus' => 'Lulus',
                                'Tidak Lulus' => 'Tidak Lulus',
                                'Mengulang' => 'Mengulang', // Optional: if applicable
                            ])
                            ->default('Lulus')
                            ->native(false),
                    ])
                    ->columns(2), // Arrange fields in 2 columns
            ]);
    }

    public static function table(Table $table): Table
    {
        return $table
            ->columns([
                Tables\Columns\TextColumn::make('mahasiswa.nama') // Display Mahasiswa's Name
                    ->label('Nama Mahasiswa')
                    ->searchable()
                    ->sortable(),
                Tables\Columns\TextColumn::make('matakuliah.nama_mk') // Display Mata Kuliah's Name
                    ->label('Mata Kuliah')
                    ->searchable()
                    ->sortable(),
                Tables\Columns\TextColumn::make('dosen.nama_dosen') // Display Dosen's Name
                    ->label('Dosen Pengampu')
                    ->searchable()
                    ->sortable(),
                Tables\Columns\TextColumn::make('semester.tahun_ajaran') // Display Semester Tahun Ajaran
                    ->label('Tahun Ajaran')
                    ->sortable(),
                Tables\Columns\TextColumn::make('semester.semester') // Display Semester Nama
                    ->label('Semester')
                    ->sortable(),
                Tables\Columns\TextColumn::make('nilai_akhir')
                    ->label('Nilai Akhir')
                    ->numeric()
                    ->sortable()
                    ->badge() // Display nilai_akhir as a badge
                    ->color(fn (float $state): string => match (true) {
                        $state >= 80 => 'success', // A
                        $state >= 70 => 'info',    // B
                        $state >= 60 => 'warning', // C
                        default => 'danger',       // D/E
                    }),
                Tables\Columns\IconColumn::make('status_kelulusan') // Use IconColumn for status
                    ->label('Status Kelulusan')
                    ->boolean() // Interpret 'Lulus' as true, others as false for icon
                    ->trueIcon('heroicon-o-check-circle')
                    ->falseIcon('heroicon-o-x-circle')
                    ->trueColor('success')
                    ->falseColor('danger')
                    ->sortable(),
                Tables\Columns\TextColumn::make('created_at')
                    ->label('Dibuat Pada')
                    ->dateTime('d M Y, H:i')
                    ->sortable()
                    ->toggleable(isToggledHiddenByDefault: true),
                Tables\Columns\TextColumn::make('updated_at')
                    ->label('Diperbarui Pada')
                    ->dateTime('d M Y, H:i')
                    ->sortable()
                    ->toggleable(isToggledHiddenByDefault: true),
            ])
            ->defaultSort('created_at', 'desc') // Default sort by most recent record
            ->filters([
                Tables\Filters\SelectFilter::make('id_mahasiswa')
                    ->label('Filter per Mahasiswa')
                    ->relationship('mahasiswa', 'nama')
                    ->searchable()
                    ->preload(),
                Tables\Filters\SelectFilter::make('id_matakuliah')
                    ->label('Filter per Mata Kuliah')
                    ->relationship('matakuliah', 'nama_mk')
                    ->searchable()
                    ->preload(),
                Tables\Filters\SelectFilter::make('id_dosen')
                    ->label('Filter per Dosen')
                    ->relationship('dosen', 'nama_dosen')
                    ->searchable()
                    ->preload(),
                Tables\Filters\SelectFilter::make('id_semester')
                    ->label('Filter per Semester')
                    ->relationship('semester', 'tahun_ajaran')
                    ->getOptionLabelUsing(fn (DimSemester $record) => "{$record->tahun_ajaran} - {$record->semester}")
                    ->searchable()
                    ->preload(),
                Tables\Filters\SelectFilter::make('status_kelulusan')
                    ->label('Filter per Status Kelulusan')
                    ->options([
                        'Lulus' => 'Lulus',
                        'Tidak Lulus' => 'Tidak Lulus',
                        'Mengulang' => 'Mengulang',
                    ]),
                Tables\Filters\Filter::make('nilai_akhir_range')
                    ->label('Filter per Nilai Akhir')
                    ->form([
                        Forms\Components\TextInput::make('min_nilai')
                            ->numeric()
                            ->placeholder('Min Nilai'),
                        Forms\Components\TextInput::make('max_nilai')
                            ->numeric()
                            ->placeholder('Max Nilai'),
                    ])
                    ->query(function (Builder $query, array $data): Builder {
                        return $query
                            ->when(
                                $data['min_nilai'],
                                fn (Builder $query, $nilai): Builder => $query->where('nilai_akhir', '>=', $nilai),
                            )
                            ->when(
                                $data['max_nilai'],
                                fn (Builder $query, $nilai): Builder => $query->where('nilai_akhir', '<=', $nilai),
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
                Tables\Grouping\Group::make('mahasiswa.nama')
                    ->label('Mahasiswa')
                    ->collapsible(),
                Tables\Grouping\Group::make('matakuliah.nama_mk')
                    ->label('Mata Kuliah')
                    ->collapsible(),
                Tables\Grouping\Group::make('semester.tahun_ajaran')
                    ->label('Tahun Ajaran')
                    ->collapsible(),
            ]);
    }

    public static function getRelations(): array
    {
        return [
            // Relation managers for related data
            // RelationManagers\SomeOtherRelationManager::class,
        ];
    }

    public static function getPages(): array
    {
        return [
            'index' => Pages\ListFaktaNilais::route('/'),
            'create' => Pages\CreateFaktaNilai::route('/create'),
            'edit' => Pages\EditFaktaNilai::route('/{record}/edit'),
        ];
    }

    public static function getNavigationBadge(): ?string
    {
        return static::getModel()::count();
    }

    public static function getNavigationBadgeColor(): ?string
    {
        return static::getModel()::count() > 200 ? 'success' : 'warning'; // Adjust threshold as needed
    }
}