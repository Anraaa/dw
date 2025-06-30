<?php

namespace App\Filament\Admin\Resources;

use App\Filament\Admin\Resources\DimMataKuliahResource\Pages;
use App\Models\DimMataKuliah;
use Filament\Forms;
use Filament\Forms\Form;
use Filament\Resources\Resource;
use Filament\Tables;
use Filament\Tables\Table;
use Illuminate\Database\Eloquent\Builder;
use Illuminate\Database\Eloquent\SoftDeletingScope;
use Filament\Tables\Filters\SelectFilter; // Pastikan ini ada
use Filament\Tables\Filters\Filter;       // Pastikan ini ada
// Tidak perlu use Filament\Tables\Filters\QueryBuilder\Constraints\TextInputConstraint; dll.

class DimMataKuliahResource extends Resource
{
    protected static ?string $model = DimMataKuliah::class;

    protected static ?string $navigationIcon = 'heroicon-o-book-open';
    protected static ?string $modelLabel = 'Mata Kuliah';
    protected static ?string $pluralModelLabel = 'Mata Kuliah';
    protected static ?string $navigationLabel = 'Data Mata Kuliah';
    protected static ?string $navigationGroup = 'Data Akademik';
    protected static ?int $navigationSort = 3;

    public static function form(Form $form): Form
    {
        return $form
            ->schema([
                Forms\Components\Section::make('Informasi Mata Kuliah')
                    ->description('Detail dasar untuk mata kuliah')
                    ->schema([
                        Forms\Components\TextInput::make('kode_mk')
                            ->label('Kode Mata Kuliah')
                            ->required()
                            ->unique(ignoreRecord: true)
                            ->maxLength(255)
                            ->placeholder('Contoh: TI101'),

                        Forms\Components\TextInput::make('nama_mk')
                            ->label('Nama Mata Kuliah')
                            ->required()
                            ->maxLength(255)
                            ->columnSpanFull(),

                        Forms\Components\TextInput::make('sks')
                            ->label('Jumlah SKS')
                            ->required()
                            ->numeric()
                            ->minValue(1)
                            ->maxValue(6)
                            ->placeholder('Contoh: 3'),

                        Forms\Components\TextInput::make('semester_mk')
                            ->label('Semester Mata Kuliah')
                            ->required()
                            ->numeric()
                            ->minValue(1)
                            ->maxValue(8)
                            ->placeholder('Contoh: 3'),
                    ])
                    ->columns(2),
            ]);
    }

    public static function table(Table $table): Table
    {
        return $table
            ->columns([
                Tables\Columns\TextColumn::make('kode_mk')
                    ->label('Kode MK')
                    ->searchable()
                    ->sortable(),

                Tables\Columns\TextColumn::make('nama_mk')
                    ->label('Nama Mata Kuliah')
                    ->searchable()
                    ->sortable(),

                Tables\Columns\TextColumn::make('sks')
                    ->label('SKS')
                    ->numeric()
                    ->sortable(),

                Tables\Columns\TextColumn::make('semester_mk')
                    ->label('Semester MK')
                    ->numeric()
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
            ->defaultSort('kode_mk', 'asc')
            ->filters([
                // Menggunakan Filter::make dengan TextInput di dalamnya (opsi jika QueryBuilder tidak dipakai)
                Tables\Filters\Filter::make('nama_mk')
                    ->label('Cari Nama MK')
                    ->form([
                        Forms\Components\TextInput::make('search_nama_mk')
                            ->placeholder('Cari nama mata kuliah'),
                    ])
                    ->query(function (Builder $query, array $data): Builder {
                        return $query->when(
                            $data['search_nama_mk'],
                            fn (Builder $query, $value): Builder => $query->where('nama_mk', 'like', "%{$value}%"),
                        );
                    }),
                
                Tables\Filters\SelectFilter::make('sks')
                    ->label('Filter per SKS')
                    ->options(
                        DimMataKuliah::distinct()->pluck('sks', 'sks')
                            ->mapWithKeys(fn ($sks) => [$sks => $sks . ' SKS'])
                            ->toArray()
                    )
                    ->searchable(),
                Tables\Filters\SelectFilter::make('semester_mk')
                    ->label('Filter per Semester')
                    ->options(
                        DimMataKuliah::distinct()->pluck('semester_mk', 'semester_mk')
                            ->mapWithKeys(fn ($semester) => [$semester => 'Semester ' . $semester])
                            ->toArray()
                    )
                    ->searchable(),
                // Tables\Filters\TrashedFilter::make(),
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
            ->groups([
                Tables\Grouping\Group::make('semester_mk')
                    ->label('Semester')
                    ->collapsible(),
                Tables\Grouping\Group::make('sks')
                    ->label('Berdasarkan SKS')
                    ->collapsible(),
            ]);
    }

    public static function getRelations(): array
    {
        return [
            // RelationManagers\DosenPengampuRelationManager::class,
        ];
    }

    public static function getPages(): array
    {
        return [
            'index' => Pages\ListDimMataKuliahs::route('/'),
            'create' => Pages\CreateDimMataKuliah::route('/create'),
            'edit' => Pages\EditDimMataKuliah::route('/{record}/edit'),
        ];
    }

    public static function getNavigationBadge(): ?string
    {
        return static::getModel()::count();
    }

    public static function getNavigationBadgeColor(): ?string
    {
        return static::getModel()::count() > 50 ? 'success' : 'warning';
    }
}