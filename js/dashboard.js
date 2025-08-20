// Global data storage
let dashboardData = {
    lastUpdated: "August 19, 2025",
    metrics: {
        avgMargin: 32.21,
        totalProfit: 5446368.847,
        totalRevenue: 14444187.626,
        losingItems: 367
    },
    trendsData: [
        { period: "360d", overallSO: 74.98, overallPO: 54.51 },
        { period: "180d", overallSO: 77.97, overallPO: 55.32 },
        { period: "90d", overallSO: 81.11, overallPO: 57.52 },
        { period: "60d", overallSO: 83.71, overallPO: 59.41 },
        { period: "30d", overallSO: 89.77, overallPO: 60.70 }
    ],
    comparisonData: [
        { period: "360d", newSO: 75.46, recertSO: 72.62, newPO: 53.54, recertPO: 74.52 },
        { period: "180d", newSO: 76.92, recertSO: 85.33, newPO: 52.98, recertPO: 118.12 },
        { period: "90d", newSO: 79.31, recertSO: 96.26, newPO: 54.07, recertPO: 165.80 },
        { period: "60d", newSO: 80.74, recertSO: 111.50, newPO: 54.46, recertPO: 219.80 },
        { period: "30d", newSO: 87.07, recertSO: 124.23, newPO: 56.23, recertPO: 249.47 }
    ]
};

let costData = null;
let salesData = null;
let customerData = null;
let supplierData = null;
let trendsChart, comparisonChart;

// Initialize dashboard
document.addEventListener('DOMContentLoaded', function() {
    updateDashboard();
    initializeCharts();
    setupFileUploads();
});

// Setup file upload functionality
function setupFileUploads() {
    document.getElementById('csvFileInput').addEventListener('change', handleCostDataUpload);
    document.getElementById('salesFileInput').addEventListener('change', handleSalesDataUpload);
    document.getElementById('customerFileInput').addEventListener('change', handleCustomerDataUpload);
    document.getElementById('supplierFileInput').addEventListener('change', handleSupplierDataUpload);
}

// Handle cost data (CSV) upload
function handleCostDataUpload(event) {
    const file = event.target.files[0];
    if (!file) return;

    showStatus('Processing cost data...', 'info');

    Papa.parse(file, {
        header: true,
        dynamicTyping: true,
        skipEmptyLines: true,
        complete: function(results) {
            try {
                costData = processCostData(results.data);
                updateDataStatus('cost', true);
                showStatus('✅ Cost data loaded successfully!', 'success');
                
                updateDashboardFromCostData();
                
                if (salesData) {
                    performInventoryOptimization();
                    performPredictiveAnalytics();
                }
            } catch (error) {
                showStatus('❌ Error processing cost data: ' + error.message, 'error');
            }
        },
        error: function(error) {
            showStatus('❌ Error reading cost file: ' + error.message, 'error');
        }
    });
}

// Handle sales data (Excel) upload
function handleSalesDataUpload(event) {
    const file = event.target.files[0];
    if (!file) return;

    showStatus('Processing sales data...', 'info');

    const reader = new FileReader();
    reader.onload = function(e) {
        try {
            const data = new Uint8Array(e.target.result);
            const workbook = XLSX.read(data, { type: 'array' });
            
            const worksheet = workbook.Sheets[workbook.SheetNames[0]];
            const rawData = XLSX.utils.sheet_to_json(worksheet, { 
                range: 7,
                header: ['Item', 'ItemDesc', 'QtySold', 'TotalRevenue']
            });
            
            salesData = processSalesData(rawData);
            updateDataStatus('sales', true);
            showStatus('✅ Sales data loaded successfully!', 'success');
            
            if (costData) {
                performInventoryOptimization();
                performPredictiveAnalytics();
            }
        } catch (error) {
            showStatus('❌ Error processing sales data: ' + error.message, 'error');
        }
    };
    reader.readAsArrayBuffer(file);
}

// Handle customer data (Excel) upload
function handleCustomerDataUpload(event) {
    const file = event.target.files[0];
    if (!file) return;

    showStatus('Processing customer data...', 'info');

    const reader = new FileReader();
    reader.onload = function(e) {
        try {
            const data = new Uint8Array(e.target.result);
            const workbook = XLSX.read(data, { type: 'array' });
            
            const worksheet = workbook.Sheets[workbook.SheetNames[0]];
            const rawData = XLSX.utils.sheet_to_json(worksheet, { 
                range: 6,
                header: ['Customer', 'TransactionType', 'Date', 'DocumentNumber', 'Memo', 'TotalRevenue']
            });
            
            customerData = processCustomerData(rawData);
            updateDataStatus('customer', true);
            showStatus('✅ Customer data loaded successfully!', 'success');
            
            performCustomerAnalysis();
        } catch (error) {
            showStatus('❌ Error processing customer data: ' + error.message, 'error');
        }
    };
    reader.readAsArrayBuffer(file);
}

// Handle supplier data (Excel) upload
function handleSupplierDataUpload(event) {
    const file = event.target.files[0];
    if (!file) return;

    showStatus('Processing supplier data...', 'info');

    const reader = new FileReader();
    reader.onload = function(e) {
        try {
            const data = new Uint8Array(e.target.result);
            const workbook = XLSX.read(data, { type: 'array' });
            
            const worksheet = workbook.Sheets[workbook.SheetNames[0]];
            const rawData = XLSX.utils.sheet_to_json(worksheet, { 
                range: 6,
                header: ['Vendor', 'Item', 'TransactionType', 'Date', 'DocumentNumber', 'Memo', 'TotalCost', 'Quantity']
            });
            
            supplierData = processSupplierData(rawData);
            updateDataStatus('supplier', true);
            showStatus('✅ Supplier data loaded successfully!', 'success');
            
            performSupplierAnalysis();
        } catch (error) {
            showStatus('❌ Error processing supplier data: ' + error.message, 'error');
        }
    };
    reader.readAsArrayBuffer(file);
}

// Process cost data
function processCostData(data) {
    const cleanedData = data.map(row => {
        const cleaned = { ...row };
        Object.keys(cleaned).forEach(key => {
            if (cleaned[key] === "—") {
                cleaned[key] = null;
            }
        });
        return cleaned;
    });

    const validItems = cleanedData.filter(item => 
        item['Average Item Rate'] && item['Average of Est. Unit Cost']
    ).map(item => {
        const rate = item['Average Item Rate'];
        const cost = item['Average of Est. Unit Cost'];
        const quantity = item.Quantity || 0;
        
        return {
            ...item,
            itemCode: item.Item ? item.Item.split(' : ')[0] : '',
            profitMargin: ((rate - cost) / rate * 100),
            profitPerUnit: (rate - cost),
            totalProfit: (rate - cost) * quantity,
            totalRevenue: rate * quantity,
            totalCost: cost * quantity,
            itemType: item.Item && item.Item.includes('ReCert') ? 'ReCert' : 'New'
        };
    });

    return validItems;
}

// Process sales data
function processSalesData(rawData) {
    const salesRecords = rawData.filter(row => 
        row.Item && 
        typeof row.QtySold === 'number' && 
        row.QtySold > 0 &&
        row.Item !== 'Inventory Item'
    ).map(row => ({
        item: row.Item.toString().trim(),
        description: row.ItemDesc || '',
        qtySold: row.QtySold,
        totalRevenue: row.TotalRevenue || 0
    }));

    const salesByItem = {};
    salesRecords.forEach(record => {
        const item = record.item;
        if (!salesByItem[item]) {
            salesByItem[item] = {
                item: item,
                description: record.description,
                totalQtySold: 0,
                totalRevenue: 0
            };
        }
        salesByItem[item].totalQtySold += record.qtySold;
        salesByItem[item].totalRevenue += record.totalRevenue;
    });

    return salesByItem;
}

// Process customer data
function processCustomerData(rawData) {
    const customerTotals = rawData.filter(row => 
        row.Customer && 
        row.Customer.startsWith('Total - ') &&
        row.TotalRevenue &&
        typeof row.TotalRevenue === 'number'
    );

    // Exclude intercompany transactions
    const externalCustomers = customerTotals.map(row => ({
        customer: row.Customer.replace('Total - ', ''),
        totalRevenue: row.TotalRevenue
    })).filter(customer => 
        customer.totalRevenue > 0 && 
        !customer.customer.toLowerCase().includes('ic-') &&
        !customer.customer.toLowerCase().includes('intercompany') &&
        !customer.customer.toLowerCase().includes('inter-company')
    ).sort((a, b) => b.totalRevenue - a.totalRevenue);

    return externalCustomers;
}

// Process supplier data
function processSupplierData(rawData) {
    const supplierTotals = rawData.filter(row => 
        row.Vendor && 
        row.Vendor.startsWith('Total - ') &&
        row.TotalCost &&
        typeof row.TotalCost === 'number'
    );

    // Process supplier totals and exclude internal suppliers
    const suppliers = supplierTotals.map(row => ({
        supplier: row.Vendor.replace('Total - ', ''),
        totalCost: row.TotalCost,
        totalQuantity: row.Quantity || 0
    })).filter(supplier => 
        supplier.totalCost > 0 && 
        !supplier.supplier.toLowerCase().includes('internal') &&
        !supplier.supplier.toLowerCase().includes('intercompany')
    ).sort((a, b) => b.totalCost - a.totalCost);

    // Also process individual line items for item-level analysis
    const lineItems = rawData.filter(row => 
        row.Vendor && 
        !row.Vendor.startsWith('Total - ') &&
        row.Item &&
        row.TotalCost &&
        typeof row.TotalCost === 'number' &&
        row.TotalCost > 0
    ).map(row => ({
        supplier: row.Vendor,
        item: row.Item,
        totalCost: row.TotalCost,
        quantity: row.Quantity || 0,
        date: row.Date
    }));

    return {
        suppliers: suppliers,
        lineItems: lineItems
    };
}

// Perform inventory optimization analysis
function performInventoryOptimization() {
    if (!costData || !salesData) return;

    showStatus('🔍 Analyzing inventory optimization opportunities...', 'info');

    const inventoryWithSales = costData.map(item => {
        let salesInfo = null;
        const itemCode = item.itemCode;
        
        if (salesData[itemCode]) {
            salesInfo = salesData[itemCode];
        } else if (salesData[itemCode + '-New']) {
            salesInfo = salesData[itemCode + '-New'];
        } else if (salesData[itemCode + '-ReCert']) {
            salesInfo = salesData[itemCode + '-ReCert'];
        } else if (salesData[item.Item]) {
            salesInfo = salesData[item.Item];
        }

        const annualSales = salesInfo ? salesInfo.totalQtySold : 0;
        const monthlySales = annualSales / 12;
        const daysOfInventory = monthlySales > 0 ? (item.Quantity / monthlySales * 30) : 999;

        return {
            ...item,
            annualSales,
            monthlySales,
            daysOfInventory,
            salesRevenue: salesInfo ? salesInfo.totalRevenue : 0,
            hasSalesData: !!salesInfo
        };
    });

    const slowMovers = inventoryWithSales.filter(item => 
        item.totalCost > 400 && 
        (item.daysOfInventory > 180 || item.annualSales === 0) && 
        item.Quantity > 0
    ).sort((a, b) => b.totalCost - a.totalCost);

    const deadStock = inventoryWithSales.filter(item => 
        item.annualSales === 0 && 
        item.totalCost > 200 &&
        item.Quantity > 0
    ).sort((a, b) => b.totalCost - a.totalCost);

    const totalSlowMoverValue = slowMovers.reduce((sum, item) => sum + item.totalCost, 0);
    const totalDeadStockValue = deadStock.reduce((sum, item) => sum + item.totalCost, 0);
    const totalOptimization = totalSlowMoverValue + totalDeadStockValue;
    const conservativeReduction = totalOptimization * 0.25;

    displayOptimizationResults(slowMovers, deadStock, totalOptimization, conservativeReduction);
    
    showStatus('✅ Inventory optimization analysis complete!', 'success');
}

// Perform customer analysis
function performCustomerAnalysis() {
    if (!customerData) return;

    showStatus('🔍 Analyzing customer intelligence...', 'info');

    const totalRevenue = customerData.reduce((sum, customer) => sum + customer.totalRevenue, 0);
    
    // Calculate concentration risk
    const top1Concentration = (customerData[0].totalRevenue / totalRevenue * 100);
    const top5Concentration = customerData.slice(0, 5).reduce((sum, c) => sum + c.totalRevenue, 0) / totalRevenue * 100;
    const top10Concentration = customerData.slice(0, 10).reduce((sum, c) => sum + c.totalRevenue, 0) / totalRevenue * 100;

    // Customer segmentation
    const segments = {
        'Platinum ($2,500+)': customerData.filter(c => c.totalRevenue >= 2500),
        'Gold ($1,000-$2,499)': customerData.filter(c => c.totalRevenue >= 1000 && c.totalRevenue < 2500),
        'Silver ($500-$999)': customerData.filter(c => c.totalRevenue >= 500 && c.totalRevenue < 1000),
        'Bronze ($100-$499)': customerData.filter(c => c.totalRevenue >= 100 && c.totalRevenue < 500),
        'Small (<$100)': customerData.filter(c => c.totalRevenue < 100)
    };

    displayCustomerResults(customerData, totalRevenue, top1Concentration, top5Concentration, segments);
    
    showStatus('✅ Customer intelligence analysis complete!', 'success');
}

// Perform supplier analysis
function performSupplierAnalysis() {
    if (!supplierData) return;

    showStatus('🔍 Analyzing supplier intelligence...', 'info');

    const suppliers = supplierData.suppliers;
    const totalPurchaseAmount = suppliers.reduce((sum, supplier) => sum + supplier.totalCost, 0);
    
    // Calculate concentration risk
    const top1Concentration = (suppliers[0].totalCost / totalPurchaseAmount * 100);
    const top3Concentration = suppliers.slice(0, 3).reduce((sum, s) => sum + s.totalCost, 0) / totalPurchaseAmount * 100;
    const top5Concentration = suppliers.slice(0, 5).reduce((sum, s) => sum + s.totalCost, 0) / totalPurchaseAmount * 100;

    // Analyze supplier dependency risks
    const highRiskSuppliers = suppliers.filter(s => (s.totalCost / totalPurchaseAmount * 100) > 20);
    const moderateRiskSuppliers = suppliers.filter(s => {
        const percentage = (s.totalCost / totalPurchaseAmount * 100);
        return percentage > 10 && percentage <= 20;
    });

    // Analyze item-supplier relationships for single-source risks
    const itemSupplierMap = {};
    supplierData.lineItems.forEach(item => {
        if (!itemSupplierMap[item.item]) {
            itemSupplierMap[item.item] = new Set();
        }
        itemSupplierMap[item.item].add(item.supplier);
    });

    const singleSourceItems = Object.entries(itemSupplierMap)
        .filter(([item, suppliers]) => suppliers.size === 1)
        .map(([item, suppliers]) => ({
            item: item,
            supplier: Array.from(suppliers)[0],
            totalCost: supplierData.lineItems
                .filter(li => li.item === item)
                .reduce((sum, li) => sum + li.totalCost, 0)
        }))
        .sort((a, b) => b.totalCost - a.totalCost);

    displaySupplierResults(suppliers, totalPurchaseAmount, top1Concentration, top3Concentration, 
                         highRiskSuppliers, moderateRiskSuppliers, singleSourceItems);
    
    showStatus('✅ Supplier intelligence analysis complete!', 'success');
}

// Perform predictive analytics
function performPredictiveAnalytics() {
    if (!costData || !salesData) return;

    showStatus('🔮 Analyzing predictive analytics and demand patterns...', 'info');

    // Calculate demand patterns and stockout risks
    const predictiveData = costData.map(item => {
        let salesInfo = null;
        const itemCode = item.itemCode;
        
        // Find sales data for this item
        if (salesData[itemCode]) {
            salesInfo = salesData[itemCode];
        } else if (salesData[itemCode + '-New']) {
            salesInfo = salesData[itemCode + '-New'];
        } else if (salesData[itemCode + '-ReCert']) {
            salesInfo = salesData[itemCode + '-ReCert'];
        } else if (salesData[item.Item]) {
            salesInfo = salesData[item.Item];
        }

        const annualSales = salesInfo ? salesInfo.totalQtySold : 0;
        const monthlySales = annualSales / 12;
        const weeklySales = annualSales / 52;
        const dailySales = annualSales / 365;
        
        // Calculate lead time (estimate 2-4 weeks based on item type)
        const estimatedLeadTime = item.itemType === 'ReCert' ? 28 : 21; // days
        
        // Calculate safety stock (2 weeks of sales)
        const safetyStock = weeklySales * 2;
        
        // Calculate reorder point
        const reorderPoint = (dailySales * estimatedLeadTime) + safetyStock;
        
        // Calculate days until stockout
        const daysUntilStockout = dailySales > 0 ? item.Quantity / dailySales : 999;
        
        // Calculate optimal order quantity (Economic Order Quantity simplified)
        const annualDemand = annualSales;
        const orderingCost = 50; // Estimated cost per order
        const holdingCostRate = 0.25; // 25% annual holding cost
        const holdingCost = item['Average of Est. Unit Cost'] * holdingCostRate;
        const eoq = holdingCost > 0 ? Math.sqrt((2 * annualDemand * orderingCost) / holdingCost) : monthlySales * 3;
        
        // Demand trend analysis (simplified)
        const demandTrend = annualSales > 0 ? 'Normal' : 'No Demand';
        const demandCategory = annualSales > 100 ? 'High' : annualSales > 20 ? 'Medium' : annualSales > 0 ? 'Low' : 'None';
        
        // Price optimization opportunity
        const marginGap = item.profitMargin < 30 ? (30 - item.profitMargin) : 0;
        const priceOptimization = marginGap > 0 ? (item['Average Item Rate'] * (marginGap / 100)) : 0;

        return {
            ...item,
            annualSales,
            monthlySales,
            weeklySales,
            dailySales,
            estimatedLeadTime,
            safetyStock,
            reorderPoint,
            daysUntilStockout,
            eoq: Math.round(eoq),
            demandTrend,
            demandCategory,
            priceOptimization,
            marginGap,
            hasSalesData: !!salesInfo
        };
    });

    displayPredictiveResults(predictiveData);
    
    showStatus('✅ Predictive analytics analysis complete!', 'success');
}

// Display optimization results
function displayOptimizationResults(slowMovers, deadStock, totalOptimization, conservativeReduction) {
    document.getElementById('inventoryOptimization').classList.add('active');

    document.getElementById('optimizationAmount').textContent = 
        `$${(totalOptimization / 1000000).toFixed(1)}M total optimization opportunity`;
    document.getElementById('optimizationOpportunity').textContent = 
        `Conservative target: $${(conservativeReduction / 1000000).toFixed(1)}M cash flow improvement`;

    const slowMoversHTML = slowMovers.slice(0, 15).map((item, index) => {
        const salesStatus = item.annualSales === 0 ? 'NO SALES' : 
                         item.daysOfInventory === 999 ? 'NO SALES' : 
                         `${Math.round(item.daysOfInventory)} days inventory`;
        
        return `<div class="insight-item inventory-slow">
            <strong>${item.itemCode}:</strong> $${item.totalCost.toLocaleString()} total cost<br>
            <small>${item.Quantity} qty @ $${item['Average of Est. Unit Cost']} each | ${salesStatus} | ${item.profitMargin.toFixed(1)}% margin</small>
        </div>`;
    }).join('');

    document.getElementById('slowMoversContent').innerHTML = slowMoversHTML;

    const deadStockHTML = deadStock.slice(0, 15).map((item, index) => {
        return `<div class="insight-item inventory-dead">
            <strong>${item.itemCode}:</strong> $${item.totalCost.toLocaleString()}<br>
            <small>${item.Quantity} qty @ $${item['Average of Est. Unit Cost']} each | Zero sales in 12 months</small>
        </div>`;
    }).join('');

    document.getElementById('deadStockContent').innerHTML = deadStockHTML;
}

// Display customer results
function displayCustomerResults(customers, totalRevenue, top1Conc, top5Conc, segments) {
    document.getElementById('customerIntelligence').classList.add('active');

    // Determine risk level and styling
    const riskLevel = top1Conc > 15 ? 'HIGH RISK' : top1Conc > 10 ? 'MODERATE RISK' : 'HEALTHY';
    const riskClass = top1Conc > 15 ? 'risk-high' : top1Conc > 10 ? 'risk-moderate' : 'risk-low';

    document.getElementById('customerConcentration').innerHTML = 
        `Top customer: ${top1Conc.toFixed(1)}% concentration <span class="risk-indicator ${riskClass}">${riskLevel}</span>`;
    document.getElementById('customerOpportunity').textContent = 
        `${segments['Gold ($1,000-$2,499)'].length} Gold customers ready for upselling`;

    // Top customers
    const topCustomersHTML = customers.slice(0, 10).map((customer, index) => {
        const percent = (customer.totalRevenue / totalRevenue * 100);
        const riskFlag = percent > 15 ? 'customer-risk' : percent > 10 ? 'customer-risk' : 'customer-high-value';
        
        return `<div class="insight-item ${riskFlag}">
            <strong>${index + 1}. ${customer.customer.length > 30 ? customer.customer.substring(0, 30) + '...' : customer.customer}</strong><br>
            <small>$${customer.totalRevenue.toLocaleString()} (${percent.toFixed(1)}% of revenue)</small>
        </div>`;
    }).join('');

    document.getElementById('topCustomersContent').innerHTML = topCustomersHTML;

    // Customer segmentation
    const segmentationHTML = Object.entries(segments).map(([tier, customers]) => {
        const tierRevenue = customers.reduce((sum, c) => sum + c.totalRevenue, 0);
        const percentage = (tierRevenue / totalRevenue * 100);
        
        return `<div class="insight-item customer-opportunity">
            <strong>${tier}:</strong> ${customers.length} customers<br>
            <small>$${tierRevenue.toLocaleString()} (${percentage.toFixed(1)}% of revenue)</small>
        </div>`;
    }).join('');

    document.getElementById('customerSegmentationContent').innerHTML = segmentationHTML;

    // Strategic insights
    const insightsHTML = `
        <div class="insight-item">
            <strong>🎯 Concentration Risk:</strong> ${riskLevel} - Top 5 customers = ${top5Conc.toFixed(1)}% of revenue
        </div>
        <div class="insight-item">
            <strong>💰 Upselling Opportunity:</strong> ${segments['Gold ($1,000-$2,499)'].length} Gold + ${segments['Silver ($500-$999)'].length} Silver customers ready for growth
        </div>
        <div class="insight-item">
            <strong>⚡ Efficiency Focus:</strong> ${segments['Small (<$100)'].length} small customers (${(segments['Small (<$100)'].length/customers.length*100).toFixed(1)}%) need process optimization
        </div>
        <div class="insight-item">
            <strong>📈 Revenue Projection:</strong> Est. $${(totalRevenue * 250 / 1000000).toFixed(1)}M annual revenue (based on daily average)
        </div>
    `;

    document.getElementById('customerInsightsContent').innerHTML = insightsHTML;
}

// Display supplier results
function displaySupplierResults(suppliers, totalPurchaseAmount, top1Conc, top3Conc, highRiskSuppliers, moderateRiskSuppliers, singleSourceItems) {
    document.getElementById('supplierIntelligence').classList.add('active');

    // Determine risk level and styling
    const riskLevel = top1Conc > 25 ? 'HIGH RISK' : top1Conc > 15 ? 'MODERATE RISK' : 'HEALTHY';
    const riskClass = top1Conc > 25 ? 'risk-high' : top1Conc > 15 ? 'risk-moderate' : 'risk-low';

    document.getElementById('supplierConcentration').innerHTML = 
        `Top supplier: ${top1Conc.toFixed(1)}% dependency <span class="risk-indicator ${riskClass}">${riskLevel}</span>`;
    document.getElementById('supplierOpportunity').textContent = 
        `${singleSourceItems.length} items with single-source risk identified`;

    // Top suppliers
    const topSuppliersHTML = suppliers.slice(0, 10).map((supplier, index) => {
        const percent = (supplier.totalCost / totalPurchaseAmount * 100);
        const riskFlag = percent > 25 ? 'supplier-dependency' : percent > 15 ? 'supplier-dependency' : 'supplier-performance';
        
        return `<div class="insight-item ${riskFlag}">
            <strong>${index + 1}. ${supplier.supplier.length > 30 ? supplier.supplier.substring(0, 30) + '...' : supplier.supplier}</strong><br>
            <small>$${supplier.totalCost.toLocaleString()} (${percent.toFixed(1)}% of purchases)</small>
        </div>`;
    }).join('');

    document.getElementById('topSuppliersContent').innerHTML = topSuppliersHTML;

    // Supplier risk analysis
    const riskHTML = `
        <div class="insight-item supplier-dependency">
            <strong>🚨 High Risk (>20%):</strong> ${highRiskSuppliers.length} suppliers<br>
            <small>$${highRiskSuppliers.reduce((sum, s) => sum + s.totalCost, 0).toLocaleString()} total dependency</small>
        </div>
        <div class="insight-item supplier-opportunity">
            <strong>⚠️ Moderate Risk (10-20%):</strong> ${moderateRiskSuppliers.length} suppliers<br>
            <small>$${moderateRiskSuppliers.reduce((sum, s) => sum + s.totalCost, 0).toLocaleString()} total dependency</small>
        </div>
        <div class="insight-item supplier-dependency">
            <strong>🔒 Single-Source Items:</strong> ${singleSourceItems.length} critical items<br>
            <small>Top risk: $${singleSourceItems[0] ? singleSourceItems[0].totalCost.toLocaleString() : '0'} (${singleSourceItems[0] ? singleSourceItems[0].item : 'N/A'})</small>
        </div>
    `;

    document.getElementById('supplierRiskContent').innerHTML = riskHTML;

    // Strategic insights
    const diversificationOpportunity = suppliers.length < 20 ? 'Consider expanding supplier base' : 'Good supplier diversification';
    const concentrationRisk = top3Conc > 60 ? 'CRITICAL: Top 3 suppliers control majority' : 
                            top3Conc > 40 ? 'MODERATE: Monitor top supplier concentration' : 
                            'HEALTHY: Well-distributed supplier base';

    const insightsHTML = `
        <div class="insight-item">
            <strong>📊 Concentration Analysis:</strong> ${concentrationRisk} (Top 3: ${top3Conc.toFixed(1)}%)
        </div>
        <div class="insight-item">
            <strong>🎯 Diversification Status:</strong> ${diversificationOpportunity} (${suppliers.length} active suppliers)
        </div>
        <div class="insight-item">
            <strong>⚡ Single-Source Risk:</strong> ${singleSourceItems.length} items need backup suppliers (Est. $${singleSourceItems.slice(0, 10).reduce((sum, item) => sum + item.totalCost, 0).toLocaleString()} at risk)
        </div>
        <div class="insight-item">
            <strong>💰 Negotiation Power:</strong> ${highRiskSuppliers.length > 0 ? 'LIMITED due to high dependencies' : 'STRONG due to balanced portfolio'}
        </div>
        <div class="insight-item">
            <strong>🔄 Action Priority:</strong> ${highRiskSuppliers.length > 0 ? 'Reduce top supplier dependencies' : 'Maintain supplier balance, monitor performance'}
        </div>
    `;

    document.getElementById('supplierInsightsContent').innerHTML = insightsHTML;
}

// Display predictive analytics results
function displayPredictiveResults(predictiveData) {
    document.getElementById('predictiveAnalytics').classList.add('active');

    // Filter for items with sales data for better predictions
    const itemsWithSales = predictiveData.filter(item => item.hasSalesData && item.annualSales > 0);

    // Stockout risk alerts
    const criticalStockouts = itemsWithSales.filter(item => 
        item.daysUntilStockout <= 30 && item.daysUntilStockout < 999 && item.totalCost > 1000
    ).sort((a, b) => a.daysUntilStockout - b.daysUntilStockout);

    const warningStockouts = itemsWithSales.filter(item => 
        item.daysUntilStockout > 30 && item.daysUntilStockout <= 60 && item.totalCost > 500
    ).sort((a, b) => a.daysUntilStockout - b.daysUntilStockout);

    // High demand items for forecasting
    const highDemandItems = itemsWithSales.filter(item => 
        item.demandCategory === 'High' && item.annualSales > 0
    ).sort((a, b) => b.annualSales - a.annualSales);

    // Reorder recommendations
    const urgentReorders = itemsWithSales.filter(item => 
        item.Quantity <= item.reorderPoint && item.annualSales > 0
    ).sort((a, b) => (a.Quantity - a.reorderPoint) - (b.Quantity - b.reorderPoint));

    // Price optimization opportunities
    const priceOpportunities = predictiveData.filter(item => 
        item.priceOptimization > 0 && item.annualSales > 0 && item.totalRevenue > 5000
    ).sort((a, b) => b.priceOptimization - a.priceOptimization);

    // Update summary
    document.getElementById('stockoutRisk').innerHTML = 
        `${criticalStockouts.length} critical stockout risks identified (≤30 days)`;
    document.getElementById('reorderAlerts').textContent = 
        `${urgentReorders.length} items below reorder point - immediate action needed`;

    // Stockout alerts
    const stockoutHTML = [
        ...criticalStockouts.slice(0, 8).map(item => `
            <div class="insight-item stockout-critical">
                <strong>🚨 CRITICAL: ${item.itemCode}</strong><br>
                <small>Will run out in ${Math.round(item.daysUntilStockout)} days | ${item.Quantity} qty left | ${Math.round(item.dailySales * 10) / 10}/day usage</small>
            </div>
        `),
        ...warningStockouts.slice(0, 5).map(item => `
            <div class="insight-item stockout-warning">
                <strong>⚠️ WARNING: ${item.itemCode}</strong><br>
                <small>Will run out in ${Math.round(item.daysUntilStockout)} days | ${item.Quantity} qty left | ${Math.round(item.dailySales * 10) / 10}/day usage</small>
            </div>
        `)
    ].join('');

    document.getElementById('stockoutAlertsContent').innerHTML = stockoutHTML || 
        '<div class="insight-item"><strong>✅ No critical stockout risks detected!</strong><br><small>All high-value items have sufficient inventory.</small></div>';

    // Demand forecasting
    const demandHTML = highDemandItems.slice(0, 10).map(item => `
        <div class="insight-item demand-high">
            <strong>${item.itemCode}:</strong> ${Math.round(item.monthlySales)} units/month<br>
            <small>Annual: ${item.annualSales} | Trend: ${item.demandTrend} | Category: ${item.demandCategory} Demand</small>
        </div>
    `).join('');

    document.getElementById('demandForecastContent').innerHTML = demandHTML || 
        '<div class="insight-item"><strong>📊 Analyzing demand patterns...</strong><br><small>Upload more sales data for better forecasting.</small></div>';

    // Reorder points
    const reorderHTML = urgentReorders.slice(0, 10).map(item => `
        <div class="insight-item ${item.Quantity <= item.reorderPoint * 0.5 ? 'reorder-urgent' : 'reorder-soon'}">
            <strong>${item.itemCode}:</strong> Reorder ${item.eoq} units<br>
            <small>Current: ${item.Quantity} | Reorder Point: ${Math.round(item.reorderPoint)} | EOQ: ${item.eoq}</small>
        </div>
    `).join('');

    document.getElementById('reorderPointsContent').innerHTML = reorderHTML || 
        '<div class="insight-item"><strong>✅ All items above reorder points!</strong><br><small>No immediate reordering needed.</small></div>';

    // Price optimization
    const priceHTML = priceOpportunities.slice(0, 8).map(item => `
        <div class="insight-item price-opportunity">
            <strong>${item.itemCode}:</strong> +$${item.priceOptimization.toFixed(2)} price opportunity<br>
            <small>Current margin: ${item.profitMargin.toFixed(1)}% | Target: 30% | Annual impact: $${(item.priceOptimization * item.annualSales).toLocaleString()}</small>
        </div>
    `).join('');

    document.getElementById('priceOptimizationContent').innerHTML = priceHTML || 
        '<div class="insight-item"><strong>✅ Pricing appears optimized!</strong><br><small>Most items meeting target margin thresholds.</small></div>';

    // Predictive insights
    const totalStockoutRisk = criticalStockouts.length + warningStockouts.length;
    const totalPriceOpportunity = priceOpportunities.reduce((sum, item) => sum + (item.priceOptimization * item.annualSales), 0);
    const averageDaysInventory = itemsWithSales.reduce((sum, item) => sum + (item.daysUntilStockout < 999 ? item.daysUntilStockout : 0), 0) / itemsWithSales.filter(item => item.daysUntilStockout < 999).length;

    const insightsHTML = `
        <div class="insight-item">
            <strong>🎯 Inventory Health Score:</strong> ${totalStockoutRisk === 0 ? 'EXCELLENT' : totalStockoutRisk < 5 ? 'GOOD' : 'NEEDS ATTENTION'} (${totalStockoutRisk} items at risk)
        </div>
        <div class="insight-item">
            <strong>📈 Demand Predictability:</strong> ${highDemandItems.length} items with predictable demand patterns (${(highDemandItems.length/itemsWithSales.length*100).toFixed(1)}% of active inventory)
        </div>
        <div class="insight-item">
            <strong>💰 Price Optimization Potential:</strong> $${(totalPriceOpportunity/1000).toFixed(0)}K annual revenue opportunity across ${priceOpportunities.length} items
        </div>
        <div class="insight-item">
            <strong>⏱️ Average Inventory Coverage:</strong> ${isNaN(averageDaysInventory) ? 'Calculating' : Math.round(averageDaysInventory) + ' days'} of demand in stock
        </div>
        <div class="insight-item">
            <strong>🔄 Reorder Optimization:</strong> ${urgentReorders.length === 0 ? 'Well managed - no urgent reorders needed' : urgentReorders.length + ' items need immediate reordering'}
        </div>
    `;

    document.getElementById('predictiveInsightsContent').innerHTML = insightsHTML;
}

// Update data status indicators
function updateDataStatus(type, loaded) {
    const statusMap = {
        'cost': 'costDataStatus',
        'sales': 'salesDataStatus', 
        'customer': 'customerDataStatus',
        'supplier': 'supplierDataStatus'
    };
    
    const labelMap = {
        'cost': 'Item Cost',
        'sales': 'Sales Data',
        'customer': 'Customer Data',
        'supplier': 'Supplier Data'
    };
    
    const statusElement = document.getElementById(statusMap[type]);
    const label = labelMap[type];
    
    if (loaded) {
        statusElement.className = 'status-indicator status-loaded';
        statusElement.textContent = `${label}: ✅ Loaded`;
    } else {
        statusElement.className = 'status-indicator status-missing';
        statusElement.textContent = `${label}: Not Loaded`;
    }
}

// Update dashboard from cost data
function updateDashboardFromCostData() {
    if (!costData) return;

    const avgMargin = costData.reduce((sum, item) => sum + item.profitMargin, 0) / costData.length;
    const totalProfit = costData.reduce((sum, item) => sum + item.totalProfit, 0);
    const totalRevenue = costData.reduce((sum, item) => sum + item.totalRevenue, 0);
    const losingItems = costData.filter(item => item.profitMargin < 0).length;

    dashboardData.metrics = {
        avgMargin,
        totalProfit,
        totalRevenue,
        losingItems
    };

    dashboardData.lastUpdated = new Date().toLocaleDateString('en-US', { 
        year: 'numeric', 
        month: 'long', 
        day: 'numeric' 
    });

    updateDashboard();
    updateTopBottomItems(costData);
}

// Update dashboard display
function updateDashboard() {
    document.getElementById('dataTimestamp').textContent = `📅 Data as of: ${dashboardData.lastUpdated}`;
    document.getElementById('avgMargin').textContent = `${dashboardData.metrics.avgMargin.toFixed(2)}%`;
    document.getElementById('totalProfit').textContent = `$${(dashboardData.metrics.totalProfit / 1000000).toFixed(1)}M`;
    document.getElementById('totalRevenue').textContent = `$${(dashboardData.metrics.totalRevenue / 1000000).toFixed(1)}M`;
    document.getElementById('losingItems').textContent = dashboardData.metrics.losingItems;
}

// Update top/bottom items
function updateTopBottomItems(items) {
    const sortedByProfit = [...items].sort((a, b) => b.totalProfit - a.totalProfit);
    
    const topItems = sortedByProfit.slice(0, 5);
    const topItemsHTML = topItems.map(item => {
        return `<div class="insight-item profit-positive">
            <strong>${item.itemCode}:</strong> $${Math.abs(item.totalProfit).toLocaleString()} profit 
            (${item.profitMargin.toFixed(1)}% margin, ${item.Quantity} qty)
        </div>`;
    }).join('');
    document.getElementById('topProfitItems').innerHTML = topItemsHTML;
    
    const bottomItems = sortedByProfit.slice(-5).reverse();
    const bottomItemsHTML = bottomItems.map(item => {
        const lossOrProfit = item.totalProfit < 0 ? 'loss' : 'profit';
        return `<div class="insight-item profit-negative">
            <strong>${item.itemCode}:</strong> $${Math.abs(item.totalProfit).toLocaleString()} ${lossOrProfit} 
            (${item.profitMargin.toFixed(1)}% margin, ${item.Quantity} qty)
        </div>`;
    }).join('');
    document.getElementById('bottomProfitItems').innerHTML = bottomItemsHTML;
}

// Initialize charts
function initializeCharts() {
    createTrendsChart();
    createComparisonChart();
}

// Create trends chart
function createTrendsChart() {
    const ctx = document.getElementById('trendsChart').getContext('2d');
    
    if (trendsChart) {
        trendsChart.destroy();
    }
    
    trendsChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: dashboardData.trendsData.map(d => d.period),
            datasets: [
                {
                    label: 'Overall Sales Orders',
                    data: dashboardData.trendsData.map(d => d.overallSO),
                    borderColor: '#3498db',
                    backgroundColor: 'rgba(52, 152, 219, 0.1)',
                    borderWidth: 4,
                    fill: false,
                    tension: 0.4
                },
                {
                    label: 'Overall Purchase Orders',
                    data: dashboardData.trendsData.map(d => d.overallPO),
                    borderColor: '#e74c3c',
                    backgroundColor: 'rgba(231, 76, 60, 0.1)',
                    borderWidth: 4,
                    fill: false,
                    tension: 0.4
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    position: 'top',
                    labels: {
                        font: { size: 12 }
                    }
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    title: {
                        display: true,
                        text: 'Average Price ($)',
                        font: { size: 12 }
                    }
                }
            }
        }
    });
}

// Create comparison chart
function createComparisonChart() {
    const ctx = document.getElementById('comparisonChart').getContext('2d');
    
    if (comparisonChart) {
        comparisonChart.destroy();
    }
    
    comparisonChart = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: dashboardData.comparisonData.map(d => d.period),
            datasets: [
                {
                    label: 'New Items SO',
                    data: dashboardData.comparisonData.map(d => d.newSO),
                    backgroundColor: 'rgba(46, 204, 113, 0.8)',
                    borderColor: '#2ecc71',
                    borderWidth: 2
                },
                {
                    label: 'ReCert Items SO',
                    data: dashboardData.comparisonData.map(d => d.recertSO),
                    backgroundColor: 'rgba(243, 156, 18, 0.8)',
                    borderColor: '#f39c12',
                    borderWidth: 2
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    position: 'top',
                    labels: {
                        font: { size: 10 }
                    }
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    title: {
                        display: true,
                        text: 'Average Price ($)',
                        font: { size: 12 }
                    }
                }
            }
        }
    });
}

// Show status messages
function showStatus(message, type) {
    const statusDiv = document.getElementById('statusMessage');
    statusDiv.innerHTML = `<div class="status-message status-${type}">${message}</div>`;
    
    if (type === 'success') {
        setTimeout(() => {
            statusDiv.innerHTML = '';
        }, 5000);
    }
}

// Export to PDF functionality
function exportToPDF() {
    const uploadSection = document.querySelector('.upload-section');
    const originalDisplay = uploadSection.style.display;
    uploadSection.style.display = 'none';
    
    window.print();
    
    setTimeout(() => {
        uploadSection.style.display = originalDisplay;
    }, 1000);
}