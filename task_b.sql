with all_transfers as (
    -- Получаем все транзакции
    select timestamp::date  as t_date,
           "to_address"     as receiver,
           "from_address"   as sender,
           quantity,
           transaction_hash as t_hash,
           block_number
    from ethereum.token_transfers
    where contract_address = '0x97ad75064b20fb2b2447fed4fa953bf7f007a706'),
     receivers_by_first_date AS (
         -- Для каждого адреса выводим первую дату получения токена
         select receiver, min(t_date::date) as first_received_date
         from all_transfers
         group by receiver),
     daily_new_addresses AS (
         -- Считаем количество уникальных адресов по датам для новых получателей тоокена
         select first_received_date      as transfer_date,
                count(distinct receiver) as new_unique_addresses
         from receivers_by_first_date
         group by first_received_date),
     max_new_addresses AS (
         -- Находим дату с максимальным числом уникальных новых адресов получателей
         select transfer_date        as max_new_addresses_date,
                new_unique_addresses as max_new_addresses_count
         from daily_new_addresses
         order by new_unique_addresses desc
         limit 1),
     first_5_days AS (
         -- Вычисляем лаг уникальных адресов по датам
         select transfer_date,
                new_unique_addresses                                                 as current_day_unique_addresses,
                coalesce(lag(new_unique_addresses) over (order by transfer_date), 0) as previous_day_unique_addresses,
                new_unique_addresses -
                coalesce(lag(new_unique_addresses) over (order by transfer_date), 0) as difference
         from daily_new_addresses
         order by transfer_date
         limit 5),
     first_5_days_alt AS (
         -- Альтернативное решение через self join
         select a.transfer_date,
                a.new_unique_addresses                           as current_day_unique_addresses_alt,
                a1.new_unique_addresses                          as previous_day_unique_addresses_alt,
                a.new_unique_addresses = a1.new_unique_addresses as difference_alt
         from daily_new_addresses a
                  join daily_new_addresses a1 on a.transfer_date = a1.transfer_date - interval '1 days'
         order by a.transfer_date
         limit 5),
     top_address as (
         -- Адрес кошелька и транзакция крупнейшего держателя
         with max_holder as (select receiver, t_hash, sum(quantity) as total
                             from all_transfers
                             where quantity >= 0.2 * 1e18
                               and block_number <= 21600000 -- Только до блока 21600000
                             group by receiver, t_hash)
         select receiver as top_address, t_hash as hash, total
         from max_holder
         order by total desc
         limit 1)

-- Выводим результаты всех запросов в одном месте
select '1. unique_receivers'    as name,
       count(distinct receiver) as value
from all_transfers
union
select '2. max_new_addresses_date' as name,
       max_new_addresses_date      as value
from max_new_addresses
union
select '2. max_new_addresses_count' as name,
       max_new_addresses_count      as value
from max_new_addresses
union
select '3. first_5_days_difference' as name,
       difference                   as value
from first_5_days
union
select '4. first_5_days_difference_alternative' as name,
       difference_alt                           as value
from first_5_days_alt
union
select '5. top_address' as name,
       top_address      as value
from top_address
union
select '5. top_address_total' as name,
       total                  as value
from top_address;