with flat_inputs as (
    select transactions.`hash`, transactions.block_timestamp, inputs.*
    from {{params.dataset_name_raw}}.transactions as transactions,
    unnest(inputs) as inputs
    where true
        {% if not params.load_all_partitions %}
        and date(timestamp_seconds(transactions.block_timestamp)) = '{{ds}}'
        {% endif %}
),
flat_outputs as (
    select transactions.`hash`, transactions.block_timestamp, outputs.*
    from {{params.dataset_name_raw}}.transactions as transactions,
    unnest(outputs) as outputs
),
enriched_flat_inputs as (
    select
        flat_inputs.index,
        flat_inputs.`hash`,
        flat_inputs.block_timestamp,
        flat_outputs.required_signatures,
        coalesce(flat_outputs.type, flat_inputs.type) as type,
        coalesce(flat_outputs.addresses, flat_inputs.addresses) as addresses,
        coalesce(flat_outputs.value, flat_inputs.value) as value
    from flat_inputs
    left join flat_outputs on flat_inputs.spent_transaction_hash = flat_outputs.`hash`
        and flat_inputs.spent_output_index = flat_outputs.index
),
grouped_enriched_inputs as (
    select `hash`, block_timestamp, array_agg(struct(index, required_signatures, type, addresses, value)) as inputs
    from enriched_flat_inputs
    group by `hash`, block_timestamp
)
select
    transactions.`hash`,
    transactions.size,
    transactions.virtual_size,
    transactions.version,
    transactions.lock_time,
    transactions.block_hash,
    transactions.block_number,
    transactions.is_coinbase,
    timestamp_seconds(transactions.block_timestamp) as block_timestamp,
    date_trunc(date(timestamp_seconds(transactions.block_timestamp)), MONTH) as block_timestamp_month,
    transactions.input_count,
    transactions.output_count,
    (select sum(value) from unnest(grouped_enriched_inputs.inputs) as inputs) as input_value,
    (select sum(value) from unnest(transactions.outputs) as outputs) as output_value,
    array(
        select as struct
            inputs.index,
            inputs.spent_transaction_hash,
            inputs.spent_output_index,
            inputs.script_asm,
            inputs.script_hex,
            inputs.sequence,
            enriched_inputs.required_signatures,
            enriched_inputs.type,
            enriched_inputs.addresses,
            enriched_inputs.value
        from unnest(grouped_enriched_inputs.inputs) as enriched_inputs
        join unnest(transactions.inputs) as inputs on inputs.index = enriched_inputs.index
        order by inputs.index
    ) as inputs,
    transactions.outputs,
    if(not transactions.is_coinbase,
    (
        coalesce((select sum(value) from unnest(grouped_enriched_inputs.inputs) as inputs), 0) -
        coalesce((select sum(value) from unnest(transactions.outputs) as outputs), 0)
    ), 0) as fee
from {{params.dataset_name_raw}}.transactions as transactions
left join grouped_enriched_inputs on grouped_enriched_inputs.`hash` = transactions.`hash`
    and grouped_enriched_inputs.block_timestamp = transactions.block_timestamp
where true
    {% if not params.load_all_partitions %}
    and date(timestamp_seconds(transactions.block_timestamp)) = '{{ds}}'
    {% endif %}
