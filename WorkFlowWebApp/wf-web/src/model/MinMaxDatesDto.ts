import { Moment} from 'moment-timezone';


export interface MinMaxDatesDto {
	minDate: Moment;
	maxDate: Moment;
	nbOfImages: number;
}