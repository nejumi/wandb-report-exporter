import {AllCommunityModule, ModuleRegistry, createGrid, type ColDef} from "ag-grid-community";

ModuleRegistry.registerModules([AllCommunityModule]);

export interface GridRenderOptions {
  container: HTMLElement;
  columnDefs: ColDef[];
  rowData: Record<string, unknown>[];
}

export function renderGrid({container, columnDefs, rowData}: GridRenderOptions): void {
  container.className = "ag-theme-quartz table-shell surface";
  createGrid(container, {
    columnDefs,
    rowData,
    defaultColDef: {
      sortable: true,
      filter: true,
      resizable: true,
      minWidth: 120,
      floatingFilter: true
    },
    animateRows: true,
    pagination: true,
    paginationPageSize: 25,
    suppressColumnVirtualisation: false,
    rowSelection: {
      mode: "singleRow",
      enableClickSelection: true
    }
  });
}
