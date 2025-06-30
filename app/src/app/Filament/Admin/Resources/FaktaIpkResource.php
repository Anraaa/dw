<?php

namespace App\Filament\Admin\Resources;

use App\Filament\Admin\Resources\FaktaIpkResource\Pages;
use App\Models\FaktaIpk;
use App\Models\DimMahasiswa; // Import DimMahasiswa model for relationships in forms
use Filament\Forms;
use Filament\Forms\Form;
use Filament\Resources\Resource;
use Filament\Tables;
use Filament\Tables\Table;
use Illuminate\Database\Eloquent\Builder;
use Illuminate\Database\Eloquent\SoftDeletingScope;
use Filament\Tables\Filters\SelectFilter; // Make sure this is imported
use Filament\Tables\Filters\Filter;       // Make sure this is imported

class FaktaIpkResource extends Resource
{
    protected static ?string $model = FaktaIpk::class;

    protected static ?string $navigationIcon = 'heroicon-o-chart-bar'; // A more relevant icon for IPK
    protected static ?string $modelLabel = 'Data IPK'; // Custom singular label
    protected static ?string $pluralModelLabel = 'Data IPK'; // Custom plural label
    protected static ?string $navigationLabel = 'Data IPK'; // Custom navigation label
    protected static ?string $navigationGroup = 'Data Akademik'; // Group with other academic data
    protected static ?int $navigationSort = 5; // Order within the group

    public static function form(Form $form): Form
    {
        return $form
            ->schema([
                Forms\Components\Section::make('Informasi IPK Mahasiswa')
                    ->description('Catatan Indeks Prestasi Kumulatif (IPK) untuk mahasiswa')
                    ->schema([
                        Forms\Components\Select::make('id_mahasiswa') // Use Select to link to DimMahasiswa
                            ->label('Nama Mahasiswa')
                            ->relationship('mahasiswa', 'nama') // Assumes a 'mahasiswa' relationship in FaktaIpk model
                            ->getOptionLabelFromRecordUsing(fn (DimMahasiswa $record) => "{$record->nim} - {$record->nama}") // Display NIM - Nama
                            ->searchable()
                            ->preload() // Preload options for faster loading
                            ->required(),

                        Forms\Components\TextInput::make('total_point')
                            ->label('Total Point')
                            ->required()
                            ->numeric()
                            ->minValue(0) // Points can't be negative
                            ->placeholder('Contoh: 100'),

                        Forms\Components\TextInput::make('total_sks')
                            ->label('Total SKS')
                            ->required()
                            ->numeric()
                            ->minValue(0) // SKS can't be negative
                            ->placeholder('Contoh: 20'),

                        Forms\Components\TextInput::make('ipk')
                            ->label('IPK')
                            ->required()
                            ->numeric()
                            ->minValue(0)
                            ->maxValue(4.00) // Max IPK is usually 4.00
                            ->step(0.01) // Allow decimal values
                            ->placeholder('Contoh: 3.50'),
                    ])
                    ->columns(2), // Arrange these fields in 2 columns
            ]);
    }

    public static function table(Table $table): Table
    {
        return $table
            ->columns([
                Tables\Columns\TextColumn::make('mahasiswa.nim') // Display NIM from related Mahasiswa
                    ->label('NIM')
                    ->searchable()
                    ->sortable(),
                Tables\Columns\TextColumn::make('mahasiswa.nama') // Display Nama from related Mahasiswa
                    ->label('Nama Mahasiswa')
                    ->searchable()
                    ->sortable(),
                Tables\Columns\TextColumn::make('total_point')
                    ->label('Total Point')
                    ->numeric()
                    ->sortable(),
                Tables\Columns\TextColumn::make('total_sks')
                    ->label('Total SKS')
                    ->numeric()
                    ->sortable(),
                Tables\Columns\TextColumn::make('ipk')
                    ->label('IPK')
                    ->numeric()
                    ->sortable()
                    ->badge() // Display IPK as a badge
                    ->color(fn (float $state): string => match (true) { // Color based on IPK value
                        $state >= 3.50 => 'success',
                        $state >= 3.00 => 'info',
                        $state >= 2.50 => 'warning',
                        default => 'danger',
                    }),
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
            ->defaultSort('created_at', 'desc') // Default sort by most recently created
            ->filters([
                Tables\Filters\SelectFilter::make('mahasiswa.fakultas') // Filter by Mahasiswa's faculty
                    ->label('Filter per Fakultas')
                    ->relationship('mahasiswa', 'fakultas') // Assumes 'mahasiswa' relationship and 'fakultas' column in DimMahasiswa
                    
                    ->searchable()
                    ->preload(),
                Tables\Filters\SelectFilter::make('mahasiswa.prodi') // Filter by Mahasiswa's prodi
                    ->label('Filter per Prodi')
                    ->relationship('mahasiswa', 'prodi')
                    
                    ->searchable()
                    ->preload(),
                Tables\Filters\Filter::make('ipk_range') // Filter by IPK range
                    ->label('Filter per IPK')
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
                                fn (Builder $query, $ipk): Builder => $query->where('ipk', '>=', $ipk),
                            )
                            ->when(
                                $data['max_ipk'],
                                fn (Builder $query, $ipk): Builder => $query->where('ipk', '<=', $ipk),
                            );
                    }),
                // Tables\Filters\TrashedFilter::make(), // Uncomment if you use Soft Deletes
            ])
            ->actions([
                Tables\Actions\ActionGroup::make([ // Group actions into a dropdown
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
            ->emptyStateActions([ // Actions when the table is empty
                Tables\Actions\CreateAction::make(),
            ])
            ->groups([ // Grouping options for the table
                Tables\Grouping\Group::make('mahasiswa.fakultas')
                    ->label('Fakultas Mahasiswa')
                    ->collapsible(),
                Tables\Grouping\Group::make('mahasiswa.tahun_masuk')
                    ->label('Tahun Masuk Mahasiswa')
                    ->collapsible(),
            ]);
    }

    public static function getRelations(): array
    {
        return [
            // Example: If IPK records relate to specific semesters, you might link it here
            // RelationManagers\SemesterRelationManager::class,
        ];
    }

    public static function getPages(): array
    {
        return [
            'index' => Pages\ListFaktaIpks::route('/'),
            'create' => Pages\CreateFaktaIpk::route('/create'),
            'edit' => Pages\EditFaktaIpk::route('/{record}/edit'),
        ];
    }

    public static function getNavigationBadge(): ?string
    {
        return static::getModel()::count(); // Displays total count in navigation
    }

    public static function getNavigationBadgeColor(): ?string
    {
        return static::getModel()::count() > 500 ? 'success' : 'warning'; // Adjust threshold as needed
    }
}