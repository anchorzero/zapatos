import type { CompleteConfig } from './config';
import type { EnumData } from './enums';
type TypeContext = 'JSONSelectable' | 'Selectable' | 'Insertable' | 'Updatable' | 'Whereable';
export declare const tsTypeForPgType: (pgType: string, enums: EnumData, context: TypeContext, config: CompleteConfig) => string;
export {};
