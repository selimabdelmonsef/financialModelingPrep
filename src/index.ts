import * as fs from "fs";
import * as path from "path";
import csv from "csv-parser";
import { createObjectCsvWriter } from "csv-writer";

interface CSVRow {
  cusip: string;
  securityName: string;
  symbol: string;
  count: string;
}

interface ProcessedRow extends CSVRow {
  new_symbol: string;
}

enum RESPONSE_STATUS {
  ok = "ok",
  nok = "nok",
}

interface GENERAL_RESPONSE {
  result: RESPONSE_STATUS;
  message: string;
}

const processCusipData = async (): Promise<GENERAL_RESPONSE> => {
  try {
    const inputFilePath = path.join(
      __dirname,
      "cusip_name_symbol_map_assignment.csv"
    );
    const outputFilePath = path.join(__dirname, "processed_cusip_data.csv");

    const rows: CSVRow[] = [];
    const nameToSymbolMap = new Map<string, string>();
    const cusipToSymbolMap = new Map<string, string>();

    let recordCount = 0;

    await new Promise<void>((resolve, reject) => {
      fs.createReadStream(inputFilePath)
        .pipe(csv())
        .on("data", (data) => {
          rows.push(data as CSVRow);
          recordCount++;
          if (recordCount % 100000 === 0) {
            console.log(`Processed ${recordCount} records...`);
          }
        })
        .on("end", resolve)
        .on("error", reject);
    });

    console.log(`Loaded ${rows.length} records.`);

    // Build lookup maps for name and cusip
    rows.forEach(({ cusip, securityName, symbol }) => {
      if (securityName && symbol) {
        nameToSymbolMap.set(securityName.toLowerCase(), symbol);
        cusipToSymbolMap.set(cusip, symbol);
      }
    });

    // Process the rows to determine new_symbol
    const processedRows: ProcessedRow[] = rows.map((row) => {
      let newSymbol = row.symbol;
      const nameKey = row.securityName ? row.securityName.toLowerCase() : "";

      if (!row.symbol && nameToSymbolMap.has(nameKey)) {
        newSymbol = nameToSymbolMap.get(nameKey)!;
      }

      if (
        row.symbol &&
        nameToSymbolMap.has(nameKey) &&
        nameToSymbolMap.get(nameKey) !== row.symbol
      ) {
        newSymbol = ""; // Conflict, set as blank
      }

      return { ...row, new_symbol: newSymbol };
    });

    const csvWriter = createObjectCsvWriter({
      path: outputFilePath,
      header: [
        { id: "cusip", title: "cusip" },
        { id: "securityName", title: "securityName" },
        { id: "symbol", title: "symbol" },
        { id: "count", title: "count" },
        { id: "new_symbol", title: "new_symbol" },
      ],
    });

    await csvWriter.writeRecords(processedRows);
    console.log("Processing complete. Output saved to", outputFilePath);
    return { result: RESPONSE_STATUS.ok, message: "Processing complete." };
  } catch (error) {
    console.error("Error processing the CUSIP data:", error);
    return {
      result: RESPONSE_STATUS.nok,
      message: "Error processing the CUSIP data.",
    };
  }
};

processCusipData().then((response) => console.log(response));
